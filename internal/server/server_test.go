package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/certs"
	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange/okx"
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
