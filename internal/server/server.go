// Package server serves every simulated exchange on its real host names, ports and paths.
// One listener per port (443, 8443, …), TLS when a certificate is given; a request goes to
// the exchange that owns its Host + path, so ws.okx.com and stream.bybit.com can share
// port 443. The exchange answers in its own protocol; this package only owns the sockets.
package server

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
)

// Feed is one exchange's generator, as the server needs it.
type Feed interface {
	Subscribe(s domain.Subscriber, market string, ack []byte, snapshot bool)
	Unsubscribe(s domain.Subscriber, market string, ack []byte)
	Drop(s domain.Subscriber)
	Top(market string, n int) (asks, bids []domain.Level, seq int64)
	SendError()
}

// Route is one exchange and its generator.
type Route struct {
	Exchange exchange.Exchange
	Feed     Feed
}

// Options mirrors the WS_* / TLS settings in .env.
type Options struct {
	ListenHost   string        // e.g. 0.0.0.0
	SendBuffer   int           // frames queued per connection before sending blocks
	WriteTimeout time.Duration // a connection that cannot take a frame in this time is closed
	TLS          *tls.Config   // nil = plain ws:// and http://
	ProxyPort    int           // the HTTP proxy NiFi's proxied calls are pointed at; 0 = none
}

// Server routes requests to exchanges.
type Server struct {
	opts      Options
	log       *slog.Logger
	upgrader  websocket.Upgrader
	handlers  map[string]http.HandlerFunc // host + path → handler
	ports     []int
	tunnels   map[string]bool          // host:port the proxy may tunnel to
	listeners map[int]*portListener    // filled by Listen
	conns     map[string]*atomic.Int64 // exchange name → open WebSocket connections
	mu        sync.Mutex
	servers   []*http.Server
}

// New checks that no two exchanges claim the same host + path.
func New(routes []Route, opts Options, log *slog.Logger) (*Server, error) {
	s := &Server{
		opts: opts, log: log,
		// NiFi is not a browser; any origin is fine on a test tool.
		upgrader:  websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
		handlers:  map[string]http.HandlerFunc{},
		tunnels:   map[string]bool{},
		listeners: map[int]*portListener{},
		conns:     map[string]*atomic.Int64{},
	}
	ports := map[int]bool{}
	add := func(e domain.Endpoint, h http.HandlerFunc, owner string) error {
		k := key(e.Host, e.Path)
		if _, taken := s.handlers[k]; taken {
			return fmt.Errorf("%s: %s%s is already served by another exchange", owner, e.Host, e.Path)
		}
		s.handlers[k] = h
		s.tunnels[tunnelKey(e.Host, e.Port)] = true
		ports[e.Port] = true
		return nil
	}
	for _, r := range routes {
		ex, feed := r.Exchange, r.Feed
		count := &atomic.Int64{}
		s.conns[ex.Name()] = count
		if err := add(ex.WS(), func(w http.ResponseWriter, req *http.Request) { s.serveWS(w, req, ex, feed, count) }, ex.Name()); err != nil {
			return nil, err
		}
		if e, ok := ex.REST(); ok {
			if err := add(e, func(w http.ResponseWriter, req *http.Request) { ex.ServeREST(w, req, feed) }, ex.Name()); err != nil {
				return nil, err
			}
		}
	}
	for p := range ports {
		s.ports = append(s.ports, p)
	}
	sort.Ints(s.ports)
	return s, nil
}

// Handler routes by Host (without its port) and path.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		h, ok := s.handlers[key(host, r.URL.Path)]
		if !ok {
			http.NotFound(w, r)
			return
		}
		h(w, r)
	})
}

// Ports are the ports Listen opens.
func (s *Server) Ports() []int { return s.ports }

// Connections is how many WebSocket clients one exchange has now.
func (s *Server) Connections(exchange string) int {
	if c, ok := s.conns[exchange]; ok {
		return int(c.Load())
	}
	return 0
}

// Listen opens one listener per port, and the proxy when ProxyPort is set, and serves
// until Shutdown.
func (s *Server) Listen() error {
	for _, port := range s.ports {
		addr := net.JoinHostPort(s.opts.ListenHost, strconv.Itoa(port))
		tcp, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", addr, err)
		}
		pl := newPortListener(tcp)
		s.listeners[port] = pl
		var ln net.Listener = pl
		if s.opts.TLS != nil {
			ln = tls.NewListener(ln, s.opts.TLS)
		}
		s.serve(ln, s.Handler(), addr)
	}
	if s.opts.ProxyPort > 0 {
		addr := net.JoinHostPort(s.opts.ListenHost, strconv.Itoa(s.opts.ProxyPort))
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("listen %s (proxy): %w", addr, err)
		}
		s.serve(ln, s.proxy(), addr)
	}
	return nil
}

func (s *Server) serve(ln net.Listener, h http.Handler, addr string) {
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	s.mu.Lock()
	s.servers = append(s.servers, srv)
	s.mu.Unlock()
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("listener stopped", "addr", addr, "err", err)
		}
	}()
}

// Shutdown stops the listeners. Open WebSockets are hijacked and end with the process.
func (s *Server) Shutdown(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, srv := range s.servers {
		_ = srv.Shutdown(ctx)
	}
}

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request, ex exchange.Exchange, feed Feed, count *atomic.Int64) {
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // the upgrader already answered
	}
	c := &conn{
		id: connID(), ws: ws, feed: feed,
		queue: make(chan []byte, s.opts.SendBuffer), done: make(chan struct{}),
	}
	if k, ok := ex.(exchange.KeepaliveRules); ok {
		ka := k.Keepalive()
		c.keepalive = &ka
		c.lastPong.Store(time.Now().UnixNano())
		ws.SetPongHandler(func(string) error {
			c.lastPong.Store(time.Now().UnixNano())
			return nil
		})
	}
	count.Add(1)
	s.log.Info("websocket connected", "exchange", ex.Name(), "conn", c.id, "remote", r.RemoteAddr)

	go s.write(c, ex)
	onMessage := func(msg []byte) { ex.OnMessage(c, msg) }
	if cx, ok := ex.(exchange.Connector); ok {
		onMessage = cx.Connect(c)
	}
	for {
		_, msg, err := ws.ReadMessage()
		if err != nil {
			break
		}
		onMessage(msg)
	}

	c.close()
	feed.Drop(c)
	count.Add(-1)
	s.log.Info("websocket disconnected", "exchange", ex.Name(), "conn", c.id)
}

// write is the connection's only writer: queued frames, the exchange's own text ping
// (Heartbeater) and its keepalive (control pings, and the closes its rules call for). Pings
// are not counted as deliveries: they are not market data.
func (s *Server) write(c *conn, ex exchange.Exchange) {
	var beat, ping, pongCheck, life <-chan time.Time
	var beatMsg []byte
	if h, ok := ex.(exchange.Heartbeater); ok {
		msg, every := h.Heartbeat()
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		beat, beatMsg = ticker.C, msg
	}
	var pongTimer *time.Timer
	if k := c.keepalive; k != nil {
		ticker := time.NewTicker(k.PingEvery)
		defer ticker.Stop()
		pongTimer = time.NewTimer(k.PongTimeout)
		defer pongTimer.Stop()
		ping, pongCheck = ticker.C, pongTimer.C
		if k.MaxLife > 0 {
			lifeTimer := time.NewTimer(k.MaxLife)
			defer lifeTimer.Stop()
			life = lifeTimer.C
		}
	}
	for {
		kind, msg := websocket.TextMessage, []byte(nil)
		select {
		case <-c.done:
			return
		case msg = <-c.queue:
		case <-beat:
			msg = beatMsg
		case <-ping:
			kind, msg = websocket.PingMessage, c.keepalive.PingPayload
		case now := <-pongCheck:
			last := time.Unix(0, c.lastPong.Load())
			if wait := last.Add(c.keepalive.PongTimeout).Sub(now); wait > 0 {
				pongTimer.Reset(wait)
				continue
			}
			s.closeNormal(c, ex, "no pong")
			return
		case <-life:
			s.closeNormal(c, ex, "connection lifetime reached")
			return
		}
		_ = c.ws.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
		if err := c.ws.WriteMessage(kind, msg); err != nil {
			c.feed.SendError()
			s.log.Warn("websocket write failed, closing", "exchange", ex.Name(), "conn", c.id, "err", err)
			c.close()
			return
		}
	}
}

// closeNormal ends a connection the way the exchange's own rules do: a normal close (1000).
func (s *Server) closeNormal(c *conn, ex exchange.Exchange, reason string) {
	_ = c.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(s.opts.WriteTimeout))
	s.log.Info("websocket closed by the exchange's rules", "exchange", ex.Name(), "conn", c.id, "reason", reason)
	c.close()
}

func key(host, path string) string { return strings.ToLower(host) + path }

// conn is one WebSocket client. Only its write goroutine writes to the socket; everything
// else queues. It is both the exchange's Session and the feed's Subscriber.
type conn struct {
	id        string
	ws        *websocket.Conn
	feed      Feed
	queue     chan []byte
	done      chan struct{}
	once      sync.Once
	keepalive *exchange.Keepalive // nil = the exchange has none
	lastPong  atomic.Int64        // unix nanos of connect or the last PONG
}

func (c *conn) ID() string { return c.id }

// Send queues msg, waiting while the queue is full. False once the connection is gone.
func (c *conn) Send(msg []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.queue <- msg:
		return true
	case <-c.done:
		return false
	}
}

func (c *conn) Subscribe(market string, ack []byte, snapshot bool) {
	c.feed.Subscribe(c, market, ack, snapshot)
}
func (c *conn) Unsubscribe(market string, ack []byte) { c.feed.Unsubscribe(c, market, ack) }

func (c *conn) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.ws.Close()
	})
}

// connID is 8 hex characters, like okx's connId.
func connID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
