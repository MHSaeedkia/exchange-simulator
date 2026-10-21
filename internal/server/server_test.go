package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/certs"
	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange/bitget"
	"exchange-simulator/internal/exchange/lbank"
	"exchange-simulator/internal/exchange/nobitex"
	"exchange-simulator/internal/exchange/okx"
	"exchange-simulator/internal/exchange/ramzinex"
	"exchange-simulator/internal/exchange/wallex"
	"exchange-simulator/internal/feed"
)

var (
	okxWS   = domain.Endpoint{Host: "ws.okx.com", Port: 8443, Path: "/ws/v5/public"}
	okxREST = domain.Endpoint{Host: "www.okx.com", Port: 443, Path: "/api/v5/market/books"}
	quiet   = slog.New(slog.NewTextHandler(io.Discard, nil))
)

// newOkx runs the real okx generator behind the server, so these tests drive the fake the
// way NiFi does.
func newOkx(t *testing.T) (Route, *feed.Feed) {
	t.Helper()
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	ex := okx.New(okx.Options{ID: 8, WS: okxWS, REST: okxREST, StepMin: 1, StepMax: 5})
	f := feed.New(feed.Options{
		Encoder: ex, Shape: shape, Settings: domain.Settings{Records: 20, PeriodMS: 10, BookDepth: 5, UpdateLevels: 2},
		SeqStart: 1000, Tick: time.Millisecond, Running: true, Seed: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
	return Route{Exchange: ex, Feed: f}, f
}

func setup(t *testing.T) (*httptest.Server, *feed.Feed, *Server) {
	t.Helper()
	route, f := newOkx(t)
	s, err := New([]Route{route}, Options{SendBuffer: 1000, WriteTimeout: time.Second}, quiet)
	require.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv, f, s
}

// dial connects to the test server but asks for ws.okx.com, as NiFi's request does once the
// DNS name points here.
func dial(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+okxWS.Path,
		http.Header{"Host": []string{okxWS.Host}})
	require.NoError(t, err)
	t.Cleanup(func() { ws.Close() })
	return ws
}

type msg struct {
	Event  string `json:"event"`
	Code   string `json:"code"`
	ConnID string `json:"connId"`
	Action string `json:"action"`
	Arg    struct {
		InstID string `json:"instId"`
	} `json:"arg"`
	Data []okx.Data `json:"data"`
}

func next(t *testing.T, ws *websocket.Conn) msg {
	t.Helper()
	require.NoError(t, ws.SetReadDeadline(time.Now().Add(time.Second)))
	_, b, err := ws.ReadMessage()
	require.NoError(t, err)
	var m msg
	require.NoError(t, json.Unmarshal(b, &m), string(b))
	return m
}

// quietFor is true when nothing arrives for d. A timed-out read leaves a gorilla connection
// unusable, so call it last.
func quietFor(ws *websocket.Conn, d time.Duration) bool {
	_ = ws.SetReadDeadline(time.Now().Add(d))
	_, _, err := ws.ReadMessage()
	return err != nil
}

func send(t *testing.T, ws *websocket.Conn, s string) {
	t.Helper()
	require.NoError(t, ws.WriteMessage(websocket.TextMessage, []byte(s)))
}

func subscribe(op, instID string) string {
	return `{"op":"` + op + `","args":[{"channel":"books","instId":"` + instID + `"}]}`
}

func TestNothingIsSentBeforeASubscribe(t *testing.T) {
	srv, _, _ := setup(t)
	assert.True(t, quietFor(dial(t, srv), 100*time.Millisecond))
}

func TestSubscribeGetsAckSnapshotThenChainedUpdates(t *testing.T) {
	srv, _, s := setup(t)
	ws := dial(t, srv)
	send(t, ws, subscribe("subscribe", "BTC-USDT"))

	ack := next(t, ws)
	assert.Equal(t, "subscribe", ack.Event)
	assert.Equal(t, "BTC-USDT", ack.Arg.InstID)
	assert.Len(t, ack.ConnID, 8)
	assert.Equal(t, 1, s.Connections("okx"))

	snap := next(t, ws)
	assert.Equal(t, "snapshot", snap.Action)
	require.Len(t, snap.Data, 1)
	assert.Len(t, snap.Data[0].Asks, 5)

	last := snap.Data[0].SeqID
	for range 10 {
		u := next(t, ws)
		assert.Equal(t, "update", u.Action)
		assert.Equal(t, last, u.Data[0].PrevSeqID, "every update names the frame before it")
		last = u.Data[0].SeqID
	}
}

func TestUnsubscribeStopsTheMarket(t *testing.T) {
	srv, f, _ := setup(t)
	ws := dial(t, srv)
	send(t, ws, subscribe("subscribe", "BTC-USDT"))
	next(t, ws)
	next(t, ws)

	send(t, ws, subscribe("unsubscribe", "BTC-USDT"))
	for next(t, ws).Event != "unsubscribe" {
	}
	assert.True(t, quietFor(ws, 100*time.Millisecond), "no frames after the ack")
	assert.Zero(t, f.ActiveMarkets())
}

func TestPingPong(t *testing.T) {
	srv, _, _ := setup(t)
	ws := dial(t, srv)
	send(t, ws, "ping")
	require.NoError(t, ws.SetReadDeadline(time.Now().Add(time.Second)))
	_, b, err := ws.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, "pong", string(b))
}

func TestDisconnectForgetsTheConnection(t *testing.T) {
	srv, f, s := setup(t)
	ws := dial(t, srv)
	send(t, ws, subscribe("subscribe", "BTC-USDT"))
	next(t, ws)
	require.Eventually(t, func() bool { return f.ActiveMarkets() == 1 }, time.Second, 5*time.Millisecond)

	ws.Close()
	assert.Eventually(t, func() bool { return f.ActiveMarkets() == 0 && s.Connections("okx") == 0 }, time.Second, 5*time.Millisecond)
}

func TestRestIsRoutedByHost(t *testing.T) {
	srv, f, _ := setup(t)
	req, err := http.NewRequest(http.MethodGet, srv.URL+okxREST.Path+"?instId=ZEC-USDT&sz=3", nil)
	require.NoError(t, err)
	req.Host = okxREST.Host
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var body okx.RestBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "0", body.Code)
	assert.Len(t, body.Data[0].Asks, 3)
	assert.Equal(t, int64(1), f.Counters().RestCalls)
}

func TestUnknownHostOrPathIs404(t *testing.T) {
	srv, _, _ := setup(t)
	for _, c := range []struct{ host, path string }{
		{"ws.okx.com", "/nope"},
		{"api.wallex.ir", okxWS.Path},
	} {
		req, err := http.NewRequest(http.MethodGet, srv.URL+c.path, nil)
		require.NoError(t, err)
		req.Host = c.host
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, c)
	}
}

func TestTwoExchangesCannotClaimTheSameAddress(t *testing.T) {
	a, _ := newOkx(t)
	b, _ := newOkx(t)
	_, err := New([]Route{a, b}, Options{}, quiet)
	assert.Error(t, err)
}

func TestPortsAreEveryEndpointPort(t *testing.T) {
	route, _ := newOkx(t)
	s, err := New([]Route{route}, Options{}, quiet)
	require.NoError(t, err)
	assert.Equal(t, []int{443, 8443}, s.Ports())
}

// This is the redirect as NiFi will meet it: the client dials the REAL host name, the
// network takes it to the simulator, and TLS holds because the simulator shows a
// certificate for that name signed by the CA the client trusts.
func TestTLSWithTheRealHostName(t *testing.T) {
	dir := t.TempDir()
	files := certs.Files{
		CACert: filepath.Join(dir, "ca.crt"), CAKey: filepath.Join(dir, "ca.key"),
		Cert: filepath.Join(dir, "server.crt"), Key: filepath.Join(dir, "server.key"),
	}
	_, err := certs.Ensure(files, []string{okxWS.Host, okxREST.Host}, time.Now())
	require.NoError(t, err)
	tlsCfg, err := certs.TLSConfig(files)
	require.NoError(t, err)

	route, _ := newOkx(t)
	s, err := New([]Route{route}, Options{SendBuffer: 100, WriteTimeout: time.Second}, quiet)
	require.NoError(t, err)
	srv := httptest.NewUnstartedServer(s.Handler())
	srv.TLS = tlsCfg
	srv.StartTLS()
	t.Cleanup(srv.Close)

	caPEM, err := os.ReadFile(files.CACert)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caPEM))
	addr := strings.TrimPrefix(srv.URL, "https://")
	dialer := websocket.Dialer{
		TLSClientConfig: &tls.Config{RootCAs: roots},
		// The DNS step: ws.okx.com resolves to the simulator.
		NetDialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
	ws, _, err := dialer.Dial(okxWS.URL("wss"), nil)
	require.NoError(t, err, "TLS must verify for the real host name")
	defer ws.Close()

	send(t, ws, subscribe("subscribe", "BTC-USDT"))
	assert.Equal(t, "subscribe", next(t, ws).Event)
	assert.Equal(t, "snapshot", next(t, ws).Action)

	// A client that does not trust our CA must fail: the redirect is not silent to it.
	plain := websocket.Dialer{NetDialContext: dialer.NetDialContext}
	_, _, err = plain.Dial(okxWS.URL("wss"), nil)
	assert.Error(t, err)
}

// freePort is a port nothing listens on now. Tests that open the real listeners use it,
// because 443 and 8443 may be taken or need root.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// proxySetup opens the real listeners for an okx on free ports, with TLS and the proxy, the
// way NiFi meets them once its proxy parameter names the simulator.
func proxySetup(t *testing.T) (ws, rest domain.Endpoint, proxyURL *url.URL, roots *x509.CertPool) {
	t.Helper()
	ws = domain.Endpoint{Host: okxWS.Host, Port: freePort(t), Path: okxWS.Path}
	rest = domain.Endpoint{Host: okxREST.Host, Port: freePort(t), Path: okxREST.Path}
	dir := t.TempDir()
	files := certs.Files{
		CACert: filepath.Join(dir, "ca.crt"), CAKey: filepath.Join(dir, "ca.key"),
		Cert: filepath.Join(dir, "server.crt"), Key: filepath.Join(dir, "server.key"),
	}
	_, err := certs.Ensure(files, []string{ws.Host, rest.Host}, time.Now())
	require.NoError(t, err)
	tlsCfg, err := certs.TLSConfig(files)
	require.NoError(t, err)
	caPEM, err := os.ReadFile(files.CACert)
	require.NoError(t, err)
	roots = x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caPEM))

	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	ex := okx.New(okx.Options{ID: 8, WS: ws, REST: rest, StepMin: 1, StepMax: 5})
	f := feed.New(feed.Options{
		Encoder: ex, Shape: shape, Settings: domain.Settings{Records: 20, PeriodMS: 10, BookDepth: 5, UpdateLevels: 2},
		SeqStart: 1000, Tick: time.Millisecond, Running: true, Seed: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)

	proxyPort := freePort(t)
	s, err := New([]Route{{Exchange: ex, Feed: f}}, Options{
		ListenHost: "127.0.0.1", SendBuffer: 100, WriteTimeout: time.Second, TLS: tlsCfg, ProxyPort: proxyPort,
	}, quiet)
	require.NoError(t, err)
	require.NoError(t, s.Listen())
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	proxyURL, err = url.Parse("http://127.0.0.1:" + strconv.Itoa(proxyPort))
	require.NoError(t, err)
	return ws, rest, proxyURL, roots
}

// NiFi's okx WebSocket goes through an HTTP proxy that names the host itself, so no DNS
// trick is needed: the CONNECT to ws.okx.com reaches the simulator's own okx, TLS and all.
func TestProxyTunnelsTheWebSocketToTheSimulator(t *testing.T) {
	ws, _, proxyURL, roots := proxySetup(t)
	dialer := websocket.Dialer{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{RootCAs: roots}}
	c, _, err := dialer.Dial(ws.URL("wss"), nil)
	require.NoError(t, err)
	defer c.Close()

	send(t, c, subscribe("subscribe", "BTC-USDT"))
	assert.Equal(t, "subscribe", next(t, c).Event)
	assert.Equal(t, "snapshot", next(t, c).Action)
}

// The snapshot request's REST call (InvokeHTTP) uses the same proxy.
func TestProxyTunnelsTheRestCallToTheSimulator(t *testing.T) {
	_, rest, proxyURL, roots := proxySetup(t)
	client := http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{RootCAs: roots}}}
	resp, err := client.Get(rest.URL("https") + "?instId=BTC-USDT&sz=3")
	require.NoError(t, err)
	defer resp.Body.Close()

	var body okx.RestBody
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "0", body.Code)
	assert.Len(t, body.Data[0].Asks, 3)
}

// During a test every proxied call reaches the simulator. One for a host + port it does not
// play is refused, never sent to the real exchange.
func TestProxyRefusesWhatTheSimulatorDoesNotPlay(t *testing.T) {
	ws, rest, proxyURL, _ := proxySetup(t)
	for _, c := range []struct {
		request string
		status  int
	}{
		{"CONNECT stream.bybit.com:443 HTTP/1.1\r\nHost: stream.bybit.com:443\r\n\r\n", http.StatusForbidden},
		// A served host on a port it is not served on.
		{"CONNECT " + ws.Host + ":" + strconv.Itoa(rest.Port) + " HTTP/1.1\r\nHost: x\r\n\r\n", http.StatusForbidden},
		// Plain-http proxying: NiFi only tunnels.
		{"GET http://www.okx.com/ HTTP/1.1\r\nHost: www.okx.com\r\n\r\n", http.StatusMethodNotAllowed},
	} {
		conn, err := net.Dial("tcp", proxyURL.Host)
		require.NoError(t, err)
		_, err = conn.Write([]byte(c.request))
		require.NoError(t, err)
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		require.NoError(t, err)
		resp.Body.Close()
		conn.Close()
		assert.Equal(t, c.status, resp.StatusCode, c.request)
	}
}

// A Centrifugo exchange's server pings every client with {} from connect on, and NiFi's
// NOBITEX group answers {} (its frames of 10 bytes or less are the ping path).
func TestHeartbeatIsSentFromConnect(t *testing.T) {
	ws := domain.Endpoint{Host: "ws.nobitex.ir", Port: 443, Path: "/connection/websocket"}
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	ex := nobitex.New(nobitex.Options{ID: 1, WS: ws, PingEvery: 20 * time.Millisecond})
	f := feed.New(feed.Options{Encoder: ex, Shape: shape, Settings: domain.Settings{Records: 1, PeriodMS: 1000, BookDepth: 5}, Tick: time.Millisecond, Seed: 1})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
	s, err := New([]Route{{Exchange: ex, Feed: f}}, Options{SendBuffer: 10, WriteTimeout: time.Second}, quiet)
	require.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+ws.Path, http.Header{"Host": []string{ws.Host}})
	require.NoError(t, err)
	defer c.Close()
	for range 2 {
		require.NoError(t, c.SetReadDeadline(time.Now().Add(time.Second)))
		_, b, err := c.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, "{}", string(b))
	}
}

// wallexServer serves a wallex whose keepalive times are short enough for a test.
func wallexServer(t *testing.T, opts wallex.Options) (url string, header http.Header) {
	t.Helper()
	ws := domain.Endpoint{Host: "api.wallex.ir", Port: 443, Path: "/ws"}
	opts.ID, opts.WS = 3, ws
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	ex := wallex.New(opts)
	f := feed.New(feed.Options{Encoder: ex, Shape: shape, Settings: domain.Settings{Records: 50, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1},
		Tick: time.Millisecond, Running: true, Seed: 1})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
	s, err := New([]Route{{Exchange: ex, Feed: f}}, Options{SendBuffer: 100, WriteTimeout: time.Second}, quiet)
	require.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + ws.Path, http.Header{"Host": []string{ws.Host}}
}

// readUntilClosed reads (which also answers pings, unless the ping handler was replaced)
// until the server closes, and returns the close code and when it came.
func readUntilClosed(t *testing.T, c *websocket.Conn, limit time.Duration) (int, time.Duration) {
	t.Helper()
	start := time.Now()
	require.NoError(t, c.SetReadDeadline(start.Add(limit)))
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			var ce *websocket.CloseError
			require.ErrorAs(t, err, &ce, "the server must close, not the deadline")
			return ce.Code, time.Since(start)
		}
	}
}

// Real wallex speaks first, and every connection has its own subscribe budget.
func TestConnectorSpeaksFirst(t *testing.T) {
	url, h := wallexServer(t, wallex.Options{})
	c, _, err := websocket.DefaultDialer.Dial(url, h)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.SetReadDeadline(time.Now().Add(time.Second)))
	_, first, err := c.ReadMessage()
	require.NoError(t, err)
	assert.Contains(t, string(first), `"pingInterval":20000`)

	send(t, c, `["subscribe", {"channel": "BTCUSDT@buyDepth"}]`)
	_, b, err := c.ReadMessage()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(b), `["BTCUSDT@buyDepth",[`), string(b))
}

// Real wallex closes (1000) a connection that sent no PONG for 30 s.
func TestNoPongCloses(t *testing.T) {
	url, h := wallexServer(t, wallex.Options{PingEvery: 20 * time.Millisecond, PongTimeout: 150 * time.Millisecond})
	c, _, err := websocket.DefaultDialer.Dial(url, h)
	require.NoError(t, err)
	defer c.Close()
	pings := 0
	c.SetPingHandler(func(string) error { pings++; return nil }) // never answers

	code, after := readUntilClosed(t, c, 2*time.Second)
	assert.Equal(t, websocket.CloseNormalClosure, code)
	assert.GreaterOrEqual(t, after, 140*time.Millisecond)
	assert.Less(t, after, 400*time.Millisecond)
	assert.Positive(t, pings, "control pings came")
}

// With PONGs the connection lives until its maximum life (30 min on real wallex), then is
// closed (1000) whatever happens. NiFi's Jetty client answers PINGs by itself.
func TestPongsKeepItOpenUntilMaxLife(t *testing.T) {
	url, h := wallexServer(t, wallex.Options{PingEvery: 20 * time.Millisecond, PongTimeout: 100 * time.Millisecond, MaxLife: 400 * time.Millisecond})
	c, _, err := websocket.DefaultDialer.Dial(url, h)
	require.NoError(t, err)
	defer c.Close()

	code, after := readUntilClosed(t, c, 2*time.Second) // the default ping handler answers
	assert.Equal(t, websocket.CloseNormalClosure, code)
	assert.GreaterOrEqual(t, after, 380*time.Millisecond, "PONGs kept it open past the pong timeout")
	assert.Less(t, after, 700*time.Millisecond)
}

// Real nobitex closes the socket with 3501 "bad request" on a message it does not take.
func TestCentrifugoBadRequestCloses3501(t *testing.T) {
	ws := domain.Endpoint{Host: "ws.nobitex.ir", Port: 443, Path: "/connection/websocket"}
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	ex := nobitex.New(nobitex.Options{ID: 1, WS: ws})
	f := feed.New(feed.Options{Encoder: ex, Shape: shape, Settings: domain.Settings{Records: 1, PeriodMS: 1000, BookDepth: 5}, Tick: time.Millisecond, Seed: 1})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
	s, err := New([]Route{{Exchange: ex, Feed: f}}, Options{SendBuffer: 10, WriteTimeout: time.Second}, quiet)
	require.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+ws.Path, http.Header{"Host": []string{ws.Host}})
	require.NoError(t, err)
	defer c.Close()
	send(t, c, `hello`)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(time.Second)))
	_, _, err = c.ReadMessage()
	var ce *websocket.CloseError
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, 3501, ce.Code)
	assert.Equal(t, "bad request", ce.Text)
	assert.Eventually(t, func() bool { return s.Connections("nobitex") == 0 }, time.Second, 5*time.Millisecond)
}

// nobitexClient dials a nobitex whose ping and pong times are short enough for a test.
func nobitexClient(t *testing.T, opts nobitex.Options) *websocket.Conn {
	t.Helper()
	ws := domain.Endpoint{Host: "ws.nobitex.ir", Port: 443, Path: "/connection/websocket"}
	opts.ID, opts.WS = 1, ws
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	ex := nobitex.New(opts)
	f := feed.New(feed.Options{Encoder: ex, Shape: shape, Settings: domain.Settings{Records: 1, PeriodMS: 1000, BookDepth: 5}, Tick: time.Millisecond, Seed: 1})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
	s, err := New([]Route{{Exchange: ex, Feed: f}}, Options{SendBuffer: 10, WriteTimeout: time.Second}, quiet)
	require.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+ws.Path, http.Header{"Host": []string{ws.Host}})
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

// Seen live: no {} back within 8 s of nobitex's {} ping closes the socket 3012 "no pong".
func TestCentrifugoNoPongCloses3012(t *testing.T) {
	c := nobitexClient(t, nobitex.Options{PingEvery: 30 * time.Millisecond, PongTimeout: 50 * time.Millisecond})
	send(t, c, `{"id":1,"connect":{}}`)
	code, after := readUntilClosed(t, c, 2*time.Second) // reads, never answers {}
	assert.Equal(t, 3012, code)
	assert.GreaterOrEqual(t, after, 70*time.Millisecond, "first ping at 30 ms + 50 ms to answer")
}

// A {} nobody asked for closes the socket 3501 (all four real Centrifugo servers do).
func TestCentrifugoUnaskedPongCloses3501(t *testing.T) {
	c := nobitexClient(t, nobitex.Options{PingEvery: time.Hour})
	send(t, c, `{"id":1,"connect":{}}`)
	send(t, c, `{}`)
	code, _ := readUntilClosed(t, c, 2*time.Second)
	assert.Equal(t, 3501, code)
}

// NiFi answers every {} with {}: the socket stays.
func TestCentrifugoPongKeepsItOpen(t *testing.T) {
	c := nobitexClient(t, nobitex.Options{PingEvery: 30 * time.Millisecond, PongTimeout: 50 * time.Millisecond})
	send(t, c, `{"id":1,"connect":{}}`)
	deadline := time.Now().Add(400 * time.Millisecond)
	require.NoError(t, c.SetReadDeadline(deadline.Add(time.Second)))
	pings := 0
	for time.Now().Before(deadline) {
		_, b, err := c.ReadMessage()
		require.NoError(t, err, "still open")
		if string(b) == "{}" {
			pings++
			send(t, c, `{}`)
		}
	}
	assert.Greater(t, pings, 5)
}

// A REST path that ends in "/" serves every path under it (ramzinex puts the market inside
// the path: /exchange/api/v1.0/exchange/orderbooks/12/buys_sells).
func TestRestPrefixRoute(t *testing.T) {
	ws := domain.Endpoint{Host: "websocket.ramzinex.com", Port: 443, Path: "/websocket"}
	rest := domain.Endpoint{Host: "publicapi.ramzinex.com", Port: 443, Path: "/exchange/api/v1.0/exchange/orderbooks/"}
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	ex := ramzinex.New(ramzinex.Options{ID: 4, WS: ws, REST: rest})
	f := feed.New(feed.Options{Encoder: ex, Shape: shape, Settings: domain.Settings{Records: 1, PeriodMS: 1000, BookDepth: 5}, Tick: time.Millisecond, Seed: 1})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
	s, err := New([]Route{{Exchange: ex, Feed: f}}, Options{SendBuffer: 10, WriteTimeout: time.Second}, quiet)
	require.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	get := func(host, path string) int {
		req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		require.NoError(t, err)
		req.Host = host
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}
	assert.Equal(t, http.StatusOK, get(rest.Host, rest.Path+"12/buys_sells"))
	assert.Equal(t, int64(1), f.Counters().RestCalls)
	assert.Equal(t, http.StatusNotFound, get(rest.Host, "/other/12/buys_sells"))
	assert.Equal(t, http.StatusNotFound, get("api.wallex.ir", rest.Path+"12/buys_sells"), "the host still counts")
}

// bitgetClient dials a bitget whose idle times are short enough for a test.
func bitgetClient(t *testing.T, opts bitget.Options) *websocket.Conn {
	t.Helper()
	ws := domain.Endpoint{Host: "ws.bitget.com", Port: 443, Path: "/v2/ws/public"}
	opts.ID, opts.WS, opts.StepMin, opts.StepMax = 5, ws, 1, 1
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	ex := bitget.New(opts)
	f := feed.New(feed.Options{Encoder: ex, Shape: shape, Settings: domain.Settings{Records: 1, PeriodMS: 1000, BookDepth: 5}, Tick: time.Millisecond, Seed: 1})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
	s, err := New([]Route{{Exchange: ex, Feed: f}}, Options{SendBuffer: 10, WriteTimeout: time.Second}, quiet)
	require.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+ws.Path, http.Header{"Host": []string{ws.Host}})
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

// Real bitget drops a client that sent no "ping" for 2 minutes: no close frame (1006).
// NiFi never pings, so NiFi is cut and reconnects, as on the real exchange.
func TestIdleClientIsDropped(t *testing.T) {
	c := bitgetClient(t, bitget.Options{IdleLimit: 100 * time.Millisecond, IdleCheck: 20 * time.Millisecond})
	send(t, c, `{"op":"subscribe","args":[{"channel":"books50","instType":"SPOT","instId":"BTCUSDT"}]}`)
	start := time.Now()
	require.NoError(t, c.SetReadDeadline(start.Add(2*time.Second)))
	var err error
	for err == nil {
		_, _, err = c.ReadMessage()
	}
	var ce *websocket.CloseError
	require.ErrorAs(t, err, &ce, "not the read deadline")
	assert.Equal(t, websocket.CloseAbnormalClosure, ce.Code, "a TCP cut, no close frame")
	assert.GreaterOrEqual(t, time.Since(start), 90*time.Millisecond)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestPingKeepsItOpen(t *testing.T) {
	c := bitgetClient(t, bitget.Options{IdleLimit: 100 * time.Millisecond, IdleCheck: 20 * time.Millisecond})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 12 {
			if c.WriteMessage(websocket.TextMessage, []byte("ping")) != nil {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
	}()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	pongs := 0
	for pongs < 12 {
		_, b, err := c.ReadMessage()
		require.NoError(t, err, "still open after %d pongs", pongs)
		if string(b) == "pong" {
			pongs++
		}
	}
	<-done
}

// lbankClient dials an lbank whose ping times are short enough for a test.
func lbankClient(t *testing.T, opts lbank.Options) *websocket.Conn {
	t.Helper()
	ws := domain.Endpoint{Host: "www.lbank.com", Port: 443, Path: "/old-wss/ccws/ws/V3"}
	opts.ID, opts.WS = 9, ws
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	ex := lbank.New(opts)
	f := feed.New(feed.Options{Encoder: ex, Shape: shape, Settings: domain.Settings{Records: 1, PeriodMS: 1000, BookDepth: 5}, Tick: time.Millisecond, Seed: 1})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
	s, err := New([]Route{{Exchange: ex, Feed: f}}, Options{SendBuffer: 10, WriteTimeout: time.Second}, quiet)
	require.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+ws.Path, http.Header{"Host": []string{ws.Host}})
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

// Real lbank pings {"ping":"<ms>","action":"ping"} soon after connect, then every 60 s. NiFi's
// answer, {"pong":"<same>","action":"pong"}, gets no reply; an id never sent is an error.
func TestTextPingWithItsOwnIDAndPongCheck(t *testing.T) {
	c := lbankClient(t, lbank.Options{PingFirst: 10 * time.Millisecond, PingEvery: time.Hour})
	require.NoError(t, c.SetReadDeadline(time.Now().Add(time.Second)))
	_, b, err := c.ReadMessage()
	require.NoError(t, err)
	assert.Regexp(t, `^\{"ping":"\d{13}","action":"ping"\}$`, string(b))

	pong := strings.NewReplacer(`"ping":`, `"pong":`, `"action":"ping"`, `"action":"pong"`).Replace(string(b))
	send(t, c, pong)
	send(t, c, pong)
	send(t, c, `{"pong":"1","action":"pong"}`)
	_, b, err = c.ReadMessage()
	require.NoError(t, err)
	assert.Regexp(t, `^\{"SERVER":"V3","message":"\[1\] is not a ping","status":"error","TS":"[0-9T:.-]+"\}$`, string(b),
		"the two right pongs got no reply; the wrong one did")
}

// Real lbank closes 1001 "server-heartbeat-timeout" when its 7th ping is due and the 6 before
// got no pong (measured at 379.9 s). NiFi answers every ping, so NiFi is never cut.
func TestMissedPingsClose(t *testing.T) {
	c := lbankClient(t, lbank.Options{PingFirst: 10 * time.Millisecond, PingEvery: 20 * time.Millisecond})
	pings := 0
	start := time.Now()
	require.NoError(t, c.SetReadDeadline(start.Add(2*time.Second)))
	for {
		_, b, err := c.ReadMessage()
		if err != nil {
			var ce *websocket.CloseError
			require.ErrorAs(t, err, &ce)
			assert.Equal(t, 1001, ce.Code)
			assert.Equal(t, "server-heartbeat-timeout", ce.Text)
			break
		}
		if strings.Contains(string(b), `"action":"ping"`) {
			pings++
		}
	}
	assert.Equal(t, 6, pings, "six unanswered pings, closed when the seventh was due")
}
