// Package config loads runtime configuration: a .env file (if present) into the process
// environment, then process environment variables, falling back to defaults for anything
// still unset. Nothing in this tool is configured any other way.
//
// Global settings have plain names (OUTPUT_MODE, …). Each exchange has a block of its own
// under its upper-case name as a prefix (OKX_RECORDS, OKX_WS_HOST, …); the defaults of that
// block are the real exchange's addresses, so NiFi's URLs work unchanged.
package config

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"

	"exchange-simulator/internal/domain"
)

// Output modes.
const (
	ModeWebSocket = "websocket"
	ModeKafka     = "kafka"
)

// Exchange is one exchange's block.
type Exchange struct {
	Name     string // okx — also the env prefix (OKX_) and the control API's path segment
	ID       int64  // lpa-core's exchanges.id
	WS       domain.Endpoint
	REST     domain.Endpoint
	HasREST  bool
	Defaults domain.Settings // its first stage
	Shape    domain.Shape    // prices and quantities of its books
	SeqStart int64
	// SeqStepMin..SeqStepMax is the random sequence step per update. Exchanges whose
	// sequence moves by exactly one use 1..1.
	SeqStepMin int64
	SeqStepMax int64
}

// Config holds every setting main() needs to wire the tool up.
type Config struct {
	OutputMode string
	Exchanges  []Exchange // every exchange the simulator knows, in .env order
	Running    []string   // the exchanges that send data from the start

	// websocket mode: the fake exchanges.
	ListenHost     string
	TLSEnabled     bool
	TLSCACert      string
	TLSCAKey       string
	TLSCert        string
	TLSKey         string
	ProxyPort      int // the HTTP proxy NiFi's proxied calls are pointed at during a test; 0 = none
	WSSendBuffer   int // frames queued per connection before sending blocks
	WSWriteTimeout time.Duration

	// kafka mode: straight to each exchange's raw topic, markets from the DB.
	KafkaBrokers []string
	KafkaTopic   string // a pattern: {id} becomes the exchange id
	Simulation   int    // the `simulation` value NiFi would have added
	DatabaseURL  string
	MarketStatus string
	DBRefresh    time.Duration

	Tick    time.Duration // how often a generator checks what is due
	APIAddr string        // control API; port 0 = a random free port

	PrometheusURL     string // empty = no Prometheus numbers
	PromQueriesFile   string
	DockerHost        string
	MonitorContainers []string // "none" in .env = no Docker numbers
	SampleInterval    time.Duration
	ReportDir         string
}

// Exchange finds an exchange's block by name.
func (c Config) Exchange(name string) (Exchange, bool) {
	for _, e := range c.Exchanges {
		if e.Name == name {
			return e, true
		}
	}
	return Exchange{}, false
}

// Topic is an exchange's raw topic in kafka mode: ex8-raw.
func (c Config) Topic(id int64) string {
	return strings.ReplaceAll(c.KafkaTopic, "{id}", strconv.FormatInt(id, 10))
}

// Hosts are every host name the simulator answers for — what its certificate must cover.
func (c Config) Hosts() []string {
	var out []string
	for _, e := range c.Exchanges {
		for _, h := range []string{e.WS.Host, e.REST.Host} {
			if h != "" && !slices.Contains(out, h) {
				out = append(out, h)
			}
		}
	}
	return out
}

const (
	defaultOutputMode        = ModeWebSocket
	defaultRunning           = "okx"
	defaultListenHost        = "0.0.0.0"
	defaultTLSEnabled        = "true"
	defaultTLSCACert         = "certs/ca.crt"
	defaultTLSCAKey          = "certs/ca.key"
	defaultTLSCert           = "certs/server.crt"
	defaultTLSKey            = "certs/server.key"
	defaultProxyPort         = "2053" // the port of NiFi's real proxy, so a test changes only its host
	defaultWSSendBuffer      = "10000"
	defaultWSWriteTimeoutMS  = "10000"
	defaultKafkaBrokers      = "localhost:9092"
	defaultKafkaTopic        = "ex{id}-raw"
	defaultSimulation        = "1"
	defaultDatabaseURL       = "postgres://postgres:postgres@localhost:5432/markets?sslmode=disable"
	defaultMarketStatus      = "subscribe"
	defaultDBRefreshMS       = "5000"
	defaultTickMS            = "1"
	defaultAPIAddr           = "0.0.0.0:9501"
	defaultPromQueriesFile   = "queries.example.txt"
	defaultDockerHost        = "unix:///var/run/docker.sock"
	defaultMonitorContainers = "nifi,kafka,jobmanager"
	defaultSampleIntervalMS  = "5000"
	defaultReportDir         = "reports"
)

// block holds an exchange's defaults, as env strings. The addresses are the REAL ones —
// that is what makes the redirect work without touching NiFi.
type block struct {
	name, id                                   string
	wsHost, wsPort, wsPath                     string
	restHost, restPort, restPath               string // restHost "" = no REST snapshot
	records, periodMS, bookDepth, updateLevels string
	basePrice, priceStep, qtyMin, qtyMax       string
	seqStart, seqStepMin, seqStepMax           string
}

// known is every exchange the simulator can play. One is added per step (see todo.md).
var known = []block{
	{
		name: "okx", id: "8",
		wsHost: "ws.okx.com", wsPort: "8443", wsPath: "/ws/v5/public",
		restHost: "www.okx.com", restPort: "443", restPath: "/api/v5/market/books",
		records: "100", periodMS: "1000", bookDepth: "400", updateLevels: "5", // okx sends 400 levels
		basePrice: "62770", priceStep: "0.1", qtyMin: "0.00100000", qtyMax: "5.00000000",
		seqStart: "1000000000", seqStepMin: "1", seqStepMax: "100", // real okx steps by 3…960
	},
	{
		name: "nobitex", id: "1",
		wsHost: "ws.nobitex.ir", wsPort: "443", wsPath: "/connection/websocket",
		// No REST: NiFi's live NOBITEX group makes no REST call.
		records: "100", periodMS: "1000", bookDepth: "24", updateLevels: "5", // nobitex sends 24 levels, every push the whole book
		basePrice: "84000", priceStep: "0.01", qtyMin: "0.000001", qtyMax: "0.200000",
		seqStart: "100000", seqStepMin: "1", seqStepMax: "1", // a Centrifugo offset moves by one
	},
	{
		name: "bitpin", id: "2",
		wsHost: "centrifugo.bitpin.ir", wsPort: "443", wsPath: "/connection/websocket",
		// No REST: NiFi's live BITPIN group makes no REST call.
		records: "100", periodMS: "1000", bookDepth: "50", updateLevels: "5", // bitpin sends 50 levels, every push the whole book
		basePrice: "84000", priceStep: "0.01", qtyMin: "0.00000001", qtyMax: "0.20000000",
		seqStart: "3000000", seqStepMin: "1", seqStepMax: "1", // a Centrifugo offset moves by one
	},
	{
		name: "wallex", id: "3",
		wsHost: "api.wallex.ir", wsPort: "443", wsPath: "/ws",
		// No REST: NiFi's live WALLEX group makes no REST call.
		// A market is two channels (@buyDepth, @sellDepth); RECORDS counts channel frames.
		records: "100", periodMS: "1000", bookDepth: "50", updateLevels: "5", // wallex sends one whole side, 50 levels
		basePrice: "84000", priceStep: "0.01", qtyMin: "0.00000001", qtyMax: "0.20000000",
		seqStart: "1", seqStepMin: "1", seqStepMax: "1", // no sequence on the wire
	},
	{
		name: "ramzinex", id: "4",
		wsHost: "websocket.ramzinex.com", wsPort: "443", wsPath: "/websocket",
		// NiFi's snapshot branch calls {path}${pair}/buys_sells: the path ends in "/".
		restHost: "publicapi.ramzinex.com", restPort: "443", restPath: "/exchange/api/v1.0/exchange/orderbooks/",
		records: "100", periodMS: "1000", bookDepth: "50", updateLevels: "5", // ramzinex sends 50 levels, every push the whole book
		basePrice: "84000", priceStep: "0.01", qtyMin: "0.0000010", qtyMax: "0.0200000",
		seqStart: "1900000", seqStepMin: "1", seqStepMax: "1", // a Centrifugo offset moves by one
	},
	{
		name: "bitget", id: "5",
		wsHost: "ws.bitget.com", wsPort: "443", wsPath: "/v2/ws/public",
		// No REST: NiFi's live BITGET group makes no REST call.
		records: "100", periodMS: "1000", bookDepth: "50", updateLevels: "5", // books50: the whole book on every frame
		basePrice: "84000", priceStep: "0.01", qtyMin: "0.000001", qtyMax: "0.600000",
		seqStart: "941000000000", seqStepMin: "2000", seqStepMax: "12000", // a sampled book counter: steps of thousands
	},
	{
		name: "bybit", id: "6",
		wsHost: "stream.bybit.com", wsPort: "443", wsPath: "/v5/public/spot",
		// NiFi calls the REST order book through its proxy (the simulator's proxy in a test).
		restHost: "api.bybit.com", restPort: "443", restPath: "/v5/market/orderbook",
		records: "100", periodMS: "1000", bookDepth: "50", updateLevels: "5", // orderbook.50: snapshot, then deltas
		basePrice: "84000", priceStep: "0.1", qtyMin: "0.000001", qtyMax: "0.900000",
		seqStart: "346400000", seqStepMin: "1", seqStepMax: "1", // u moves by exactly one
	},
}

// Load reads envFile into the process environment — a missing file is not an error, it
// just means real env vars and defaults are used — then builds a Config. Real
// environment variables set before Load runs always win over envFile.
func Load(envFile string) (Config, error) {
	_ = godotenv.Load(envFile)
	return FromEnv()
}

// FromEnv builds a Config purely from the current process environment, without touching
// any file. Kept separate from Load so the fallback logic is unit-testable with t.Setenv.
func FromEnv() (Config, error) {
	p := parser{}
	cfg := Config{
		OutputMode:     env("OUTPUT_MODE", defaultOutputMode),
		Running:        names(env("EXCHANGES_RUNNING", defaultRunning)),
		ListenHost:     env("LISTEN_HOST", defaultListenHost),
		TLSEnabled:     p.bool("TLS_ENABLED", defaultTLSEnabled),
		TLSCACert:      env("TLS_CA_CERT", defaultTLSCACert),
		TLSCAKey:       env("TLS_CA_KEY", defaultTLSCAKey),
		TLSCert:        env("TLS_CERT", defaultTLSCert),
		TLSKey:         env("TLS_KEY", defaultTLSKey),
		ProxyPort:      int(p.int("PROXY_PORT", defaultProxyPort)),
		WSSendBuffer:   int(p.int("WS_SEND_BUFFER", defaultWSSendBuffer)),
		WSWriteTimeout: p.ms("WS_WRITE_TIMEOUT_MS", defaultWSWriteTimeoutMS),

		KafkaBrokers: list(env("KAFKA_BROKERS", defaultKafkaBrokers)),
		KafkaTopic:   env("KAFKA_TOPIC", defaultKafkaTopic),
		Simulation:   int(p.int("SIMULATION", defaultSimulation)),
		DatabaseURL:  env("DATABASE_URL", defaultDatabaseURL),
		MarketStatus: env("MARKET_STATUS", defaultMarketStatus),
		DBRefresh:    p.ms("DB_REFRESH_MS", defaultDBRefreshMS),

		Tick:    p.ms("TICK_MS", defaultTickMS),
		APIAddr: env("API_ADDR", defaultAPIAddr),

		PrometheusURL:     strings.TrimRight(env("PROMETHEUS_URL", ""), "/"),
		PromQueriesFile:   env("PROM_QUERIES_FILE", defaultPromQueriesFile),
		DockerHost:        env("DOCKER_HOST", defaultDockerHost),
		MonitorContainers: names(env("MONITOR_CONTAINERS", defaultMonitorContainers)),
		SampleInterval:    p.ms("SAMPLE_INTERVAL_MS", defaultSampleIntervalMS),
		ReportDir:         env("REPORT_DIR", defaultReportDir),
	}
	for _, b := range known {
		cfg.Exchanges = append(cfg.Exchanges, p.exchange(b))
	}
	if p.err != nil {
		return Config{}, p.err
	}
	return cfg, cfg.validate()
}

func (p *parser) exchange(b block) Exchange {
	pre := strings.ToUpper(b.name) + "_"
	e := Exchange{
		Name: b.name,
		ID:   p.int(pre+"ID", b.id),
		WS:   p.endpoint(pre+"WS_", b.wsHost, b.wsPort, b.wsPath),
		Defaults: domain.Settings{
			Records:      p.int(pre+"RECORDS", b.records),
			PeriodMS:     p.int(pre+"PERIOD_MS", b.periodMS),
			BookDepth:    int(p.int(pre+"BOOK_DEPTH", b.bookDepth)),
			UpdateLevels: int(p.int(pre+"UPDATE_LEVELS", b.updateLevels)),
		},
		SeqStart:   p.int(pre+"SEQ_START", b.seqStart),
		SeqStepMin: p.int(pre+"SEQ_STEP_MIN", b.seqStepMin),
		SeqStepMax: p.int(pre+"SEQ_STEP_MAX", b.seqStepMax),
	}
	if b.restHost != "" {
		e.REST, e.HasREST = p.endpoint(pre+"REST_", b.restHost, b.restPort, b.restPath), true
	}
	shape, err := domain.NewShape(
		env(pre+"BASE_PRICE", b.basePrice), env(pre+"PRICE_STEP", b.priceStep),
		env(pre+"QTY_MIN", b.qtyMin), env(pre+"QTY_MAX", b.qtyMax))
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%sBASE_PRICE / PRICE_STEP / QTY_MIN / QTY_MAX: %w", pre, err)
	}
	e.Shape = shape
	return e
}

func (p *parser) endpoint(pre, host, port, path string) domain.Endpoint {
	e := domain.Endpoint{Host: env(pre+"HOST", host), Port: int(p.int(pre+"PORT", port)), Path: env(pre+"PATH", path)}
	if (e.Port < 1 || e.Port > 65535 || !strings.HasPrefix(e.Path, "/")) && p.err == nil {
		p.err = fmt.Errorf("%sPORT must be 1…65535 and %sPATH must start with /", pre, pre)
	}
	return e
}

// validate fails on a bad value before anything starts.
func (c Config) validate() error {
	switch {
	case c.OutputMode != ModeWebSocket && c.OutputMode != ModeKafka:
		return fmt.Errorf("OUTPUT_MODE must be %q or %q, got %q", ModeWebSocket, ModeKafka, c.OutputMode)
	case c.OutputMode == ModeKafka && len(c.KafkaBrokers) == 0:
		return fmt.Errorf("kafka mode needs KAFKA_BROKERS")
	case c.WSSendBuffer < 1:
		return fmt.Errorf("WS_SEND_BUFFER must be at least 1")
	case c.ProxyPort < 0 || c.ProxyPort > 65535:
		return fmt.Errorf("PROXY_PORT must be 0 (no proxy) or 1…65535")
	}
	for _, name := range c.Running {
		if _, ok := c.Exchange(name); !ok {
			return fmt.Errorf("EXCHANGES_RUNNING: no exchange %q (known: %s)", name, strings.Join(c.names(), ", "))
		}
	}
	for key, d := range map[string]time.Duration{
		"WS_WRITE_TIMEOUT_MS": c.WSWriteTimeout, "DB_REFRESH_MS": c.DBRefresh,
		"TICK_MS": c.Tick, "SAMPLE_INTERVAL_MS": c.SampleInterval,
	} {
		if d <= 0 {
			return fmt.Errorf("%s must be at least 1", key)
		}
	}
	for _, e := range c.Exchanges {
		pre := strings.ToUpper(e.Name) + "_"
		switch {
		case e.SeqStart < 0:
			return fmt.Errorf("%sSEQ_START must not be negative", pre)
		case e.SeqStepMin < 1 || e.SeqStepMax < e.SeqStepMin:
			return fmt.Errorf("%sSEQ_STEP_MIN must be at least 1 and %sSEQ_STEP_MAX not below it", pre, pre)
		}
		if err := e.Defaults.Validate(e.Shape.MaxDepth()); err != nil {
			return fmt.Errorf("%s*: %w", pre, err)
		}
	}
	return nil
}

func (c Config) names() []string {
	var out []string
	for _, e := range c.Exchanges {
		out = append(out, e.Name)
	}
	return out
}

// parser keeps the first parse error, so FromEnv reads as one flat list.
type parser struct{ err error }

func (p *parser) int(key, def string) int64 {
	raw := env(key, def)
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s must be a whole number, got %q", key, raw)
	}
	return n
}

func (p *parser) ms(key, def string) time.Duration {
	return time.Duration(p.int(key, def)) * time.Millisecond
}

func (p *parser) bool(key, def string) bool {
	raw := env(key, def)
	b, err := strconv.ParseBool(raw)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s must be true or false, got %q", key, raw)
	}
	return b
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func list(raw string) []string {
	var out []string
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// names is a comma list where "none" means an empty list. An empty value already means
// "the default", so turning a list off needs a word of its own.
func names(raw string) []string {
	if raw == "none" {
		return nil
	}
	return list(raw)
}
