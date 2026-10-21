package server

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The proxy stands in for the HTTP proxy that NiFi's okx calls (and lbank's, and bybit's
// REST) go through. That proxy looks the host name up on its own machine, so the DNS
// aliases never see these calls; with NiFi's proxy host set to the simulator they come
// here. A CONNECT to a host + port the simulator serves is handed, inside the process, to
// that port's listener: TLS and routing then work as for a direct call. Anything else is
// refused, so a test never reaches a real exchange.

func tunnelKey(host string, port int) string {
	return net.JoinHostPort(strings.ToLower(host), strconv.Itoa(port))
}

func (s *Server) proxy() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "only CONNECT is supported", http.StatusMethodNotAllowed)
			return
		}
		var ln *portListener
		if host, rawPort, err := net.SplitHostPort(r.Host); err == nil {
			if port, err := strconv.Atoi(rawPort); err == nil && s.tunnels[tunnelKey(host, port)] {
				ln = s.listeners[port]
			}
		}
		if ln == nil {
			s.log.Warn("proxy refused a host the simulator does not play", "target", r.Host, "remote", r.RemoteAddr)
			http.Error(w, r.Host+" is not played by exchange-simulator", http.StatusForbidden)
			return
		}
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		_ = conn.SetDeadline(time.Time{}) // the proxy server's header timeout must not end the tunnel
		if _, err := conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n")); err != nil {
			_ = conn.Close()
			return
		}
		if rw.Reader.Buffered() > 0 { // the client did not wait for the 200
			conn = &bufferedConn{Conn: conn, r: rw.Reader}
		}
		ln.hand(conn)
	})
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// portListener is one port's TCP listener plus the connections the proxy hands it.
type portListener struct {
	net.Listener
	conns  chan net.Conn
	errs   chan error
	closed chan struct{}
	once   sync.Once
}

func newPortListener(ln net.Listener) *portListener {
	l := &portListener{Listener: ln, conns: make(chan net.Conn), errs: make(chan error), closed: make(chan struct{})}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				select {
				case l.errs <- err:
				case <-l.closed:
					return
				}
				if errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}
			l.hand(c)
		}
	}()
	return l
}

func (l *portListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case err := <-l.errs:
		return nil, err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *portListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return l.Listener.Close()
}

// hand gives the listener a connection, as if it had been dialled on this port.
func (l *portListener) hand(c net.Conn) {
	select {
	case l.conns <- c:
	case <-l.closed:
		_ = c.Close()
	}
}
