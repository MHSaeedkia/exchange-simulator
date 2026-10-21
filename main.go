// exchange-simulator plays the crypto exchanges lpa-core ingests, for load tests: each
// exchange sends its own real frames, at a rate you choose, and the tool writes how the
// system behaved at each rate.
//
// Two modes (OUTPUT_MODE in .env):
//
//   - websocket: fake exchange servers on the REAL host names, ports and paths. With the
//     host names pointed at the simulator (docs/REDIRECT.md), NiFi connects to it with no
//     flow change, and lpa-console subscribes markets as usual.
//   - kafka: straight to each exchange's raw topic, bypassing NiFi; markets come from the DB.
//
// Each exchange can be started, stopped and re-rated at runtime through the control API;
// each change starts a new report stage.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"exchange-simulator/internal/certs"
	"exchange-simulator/internal/config"
	"exchange-simulator/internal/exchange"
	"exchange-simulator/internal/exchange/okx"
	"exchange-simulator/internal/feed"
	"exchange-simulator/internal/httpapi"
	"exchange-simulator/internal/kafka"
	"exchange-simulator/internal/monitor"
	"exchange-simulator/internal/postgres"
	"exchange-simulator/internal/report"
	"exchange-simulator/internal/server"
)

func main() {
	envFile := flag.String("env", ".env", "path to the .env file")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	genCerts := flag.Bool("gen-certs", false, "make the test CA (if missing) and the server certificate, then exit")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		slog.Error("invalid log level", "value", *logLevel)
		os.Exit(1)
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if err := run(*envFile, *genCerts, log); err != nil {
		log.Error("exchange-simulator failed", "err", err)
		os.Exit(1)
	}
}

// sim is one exchange and its generator.
type sim struct {
	cfg  config.Exchange
	ex   exchange.Exchange
	feed *feed.Feed
}

func run(envFile string, genCerts bool, log *slog.Logger) error {
	cfg, err := config.Load(envFile)
	if err != nil {
		return err
	}
	if genCerts {
		return makeCerts(cfg, log)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rec, err := report.New(cfg.ReportDir, time.Now())
	if err != nil {
		return err
	}
	defer rec.Close()
	sampler, err := newSampler(cfg, log)
	if err != nil {
		return err
	}

	sims := make([]*sim, 0, len(cfg.Exchanges))
	for _, e := range cfg.Exchanges {
		ex, err := newExchange(e)
		if err != nil {
			return err
		}
		sims = append(sims, &sim{cfg: e, ex: ex})
	}

	// Kafka first: each feed publishes through it, and its error callback needs the feeds.
	var producer *kafka.Producer
	if cfg.OutputMode == config.ModeKafka {
		byTopic := map[string]*sim{}
		var topics []string
		for _, s := range sims {
			t := cfg.Topic(s.cfg.ID)
			byTopic[t] = s
			topics = append(topics, t)
		}
		producer, err = kafka.New(ctx, cfg.KafkaBrokers, topics, func(t string) { byTopic[t].feed.SendError() }, log)
		if err != nil {
			return err
		}
		defer producer.Close()
	}

	// Feeds get their own context: on shutdown the last stage is closed while they still answer.
	feedCtx, stopFeeds := context.WithCancel(context.Background())
	defer stopFeeds()
	for i, s := range sims {
		var pub feed.Publisher
		if producer != nil {
			pub = kafkaPublisher{p: producer, topic: cfg.Topic(s.cfg.ID), ex: s.ex, simulation: cfg.Simulation}
		}
		s.feed = feed.New(feed.Options{
			Encoder: s.ex, Shape: s.cfg.Shape, Settings: s.cfg.Defaults, SeqStart: s.cfg.SeqStart,
			Tick: cfg.Tick, Running: slices.Contains(cfg.Running, s.cfg.Name), Publisher: pub,
			Seed: time.Now().UnixNano() + int64(i),
		})
		go s.feed.Run(feedCtx)
	}

	apiExchanges := make([]httpapi.Exchange, 0, len(sims))
	var servers []func(context.Context)
	if cfg.OutputMode == config.ModeKafka {
		repo, err := postgres.New(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer repo.Close()
		for _, s := range sims {
			go refreshMarkets(ctx, repo, s, cfg, log)
			apiExchanges = append(apiExchanges, httpapi.Exchange{Name: s.cfg.Name, Feed: s.feed})
			log.Info("exchange", "name", s.cfg.Name, "running", s.feed.Running(), "topic", cfg.Topic(s.cfg.ID))
		}
	} else {
		srv, err := serveExchanges(cfg, sims, log)
		if err != nil {
			return err
		}
		servers = append(servers, srv.Shutdown)
		wsScheme, httpScheme := "ws", "http"
		if cfg.TLSEnabled {
			wsScheme, httpScheme = "wss", "https"
		}
		for _, s := range sims {
			ae := httpapi.Exchange{
				Name: s.cfg.Name, Feed: s.feed, WebSocketURL: s.cfg.WS.URL(wsScheme),
				Connections: func() int { return srv.Connections(s.cfg.Name) },
			}
			if s.cfg.HasREST {
				ae.RESTURL = s.cfg.REST.URL(httpScheme)
			}
			apiExchanges = append(apiExchanges, ae)
			log.Info("exchange", "name", s.cfg.Name, "running", s.feed.Running(), "websocket", ae.WebSocketURL, "rest", ae.RESTURL)
		}
	}

	states := func() map[string]report.State {
		out := map[string]report.State{}
		for _, s := range sims {
			out[s.cfg.Name] = report.State{
				Running: s.feed.Running(), Settings: s.feed.Settings(),
				Counters: s.feed.Counters(), Markets: s.feed.ActiveMarkets(),
			}
		}
		return out
	}
	api := httpapi.New(apiExchanges, rec, httpapi.Options{Mode: cfg.OutputMode, States: states, Stop: stop}, log)
	apiSrv, apiAddr, err := serve(cfg.APIAddr, api.Routes(), log)
	if err != nil {
		return err
	}
	servers = append(servers, func(ctx context.Context) { _ = apiSrv.Shutdown(ctx) })

	rec.Begin(time.Now(), states())
	log.Info("stage started", "stage", 1, "running", cfg.Running)
	log.Info("control api listening", "url", "http://"+apiAddr, "report_dir", rec.Dir())

	go sample(ctx, sampler, rec, sims, cfg.SampleInterval, log)

	<-ctx.Done()
	log.Info("stopping")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, shutdown := range servers {
		shutdown(shutdownCtx)
	}
	last, err := rec.End(time.Now(), states())
	httpapi.LogStage(log, last)
	if err != nil {
		return err
	}
	log.Info("report written", "dir", rec.Dir())
	return nil
}

// newExchange builds one exchange from its .env block. One case is added per exchange.
func newExchange(e config.Exchange) (exchange.Exchange, error) {
	switch e.Name {
	case "okx":
		return okx.New(okx.Options{ID: e.ID, WS: e.WS, REST: e.REST, StepMin: e.SeqStepMin, StepMax: e.SeqStepMax}), nil
	}
	return nil, fmt.Errorf("exchange %q has a config block but no implementation", e.Name)
}

func certFiles(cfg config.Config) certs.Files {
	return certs.Files{CACert: cfg.TLSCACert, CAKey: cfg.TLSCAKey, Cert: cfg.TLSCert, Key: cfg.TLSKey}
}

// makeCerts is -gen-certs: the first step of docs/REDIRECT.md.
func makeCerts(cfg config.Config, log *slog.Logger) error {
	created, err := certs.Ensure(certFiles(cfg), cfg.Hosts(), time.Now())
	if err != nil {
		return err
	}
	if created {
		log.Warn("NEW test CA made: put it in NiFi's truststore (docs/REDIRECT.md)", "ca", cfg.TLSCACert)
	} else {
		log.Info("test CA kept", "ca", cfg.TLSCACert)
	}
	log.Info("server certificate written", "cert", cfg.TLSCert, "hosts", cfg.Hosts())
	return nil
}

// serveExchanges opens the fake exchanges on their real ports, with TLS for the real host
// names when TLS_ENABLED.
func serveExchanges(cfg config.Config, sims []*sim, log *slog.Logger) (*server.Server, error) {
	opts := server.Options{ListenHost: cfg.ListenHost, SendBuffer: cfg.WSSendBuffer, WriteTimeout: cfg.WSWriteTimeout, ProxyPort: cfg.ProxyPort}
	if cfg.TLSEnabled {
		if err := makeCerts(cfg, log); err != nil {
			return nil, err
		}
		tlsCfg, err := certs.TLSConfig(certFiles(cfg))
		if err != nil {
			return nil, err
		}
		opts.TLS = tlsCfg
	}
	routes := make([]server.Route, 0, len(sims))
	for _, s := range sims {
		routes = append(routes, server.Route{Exchange: s.ex, Feed: s.feed})
	}
	srv, err := server.New(routes, opts, log)
	if err != nil {
		return nil, err
	}
	if err := srv.Listen(); err != nil {
		return nil, err
	}
	log.Info("fake exchanges listening", "host", cfg.ListenHost, "ports", srv.Ports(), "tls", cfg.TLSEnabled, "proxy_port", cfg.ProxyPort)
	return srv, nil
}

// kafkaPublisher is one exchange's way into Kafka: its raw topic, its NiFi envelope.
type kafkaPublisher struct {
	p          *kafka.Producer
	topic      string
	ex         exchange.Exchange
	simulation int
}

func (k kafkaPublisher) Publish(frame []byte) {
	k.p.Publish(k.topic, k.ex.Envelope(frame, kafka.NewID(), k.simulation))
}

func newSampler(cfg config.Config, log *slog.Logger) (*monitor.Sampler, error) {
	var queries []monitor.Query
	if cfg.PrometheusURL != "" {
		q, err := monitor.LoadQueries(cfg.PromQueriesFile)
		if err != nil {
			return nil, err
		}
		queries = q
	}
	log.Info("monitoring", "containers", cfg.MonitorContainers, "prometheus", cfg.PrometheusURL,
		"queries", len(queries), "every", cfg.SampleInterval)
	return monitor.New(monitor.Options{
		DockerHost: cfg.DockerHost, Containers: cfg.MonitorContainers,
		PrometheusURL: cfg.PrometheusURL, Queries: queries, Timeout: cfg.SampleInterval,
	}, log)
}

// serve listens first, so a port of 0 resolves to the real one before it is logged.
func serve(addr string, h http.Handler, log *slog.Logger) (*http.Server, string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", err
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server stopped", "addr", ln.Addr().String(), "err", err)
		}
	}()
	return srv, ln.Addr().String(), nil
}

// refreshMarkets is kafka mode's market list for one exchange: re-read every DB_REFRESH_MS.
// A failed read keeps the last list.
func refreshMarkets(ctx context.Context, repo *postgres.Repository, s *sim, cfg config.Config, log *slog.Logger) {
	var last []string
	ticker := time.NewTicker(cfg.DBRefresh)
	defer ticker.Stop()
	for {
		markets, err := repo.Markets(ctx, s.cfg.ID, cfg.MarketStatus)
		if err != nil {
			log.Warn("cannot read markets, keeping the last list", "exchange", s.cfg.Name, "err", err)
		} else {
			if !slices.Equal(markets, last) {
				log.Info("markets", "exchange", s.cfg.Name, "count", len(markets), "list", markets)
			}
			s.feed.SetMarkets(markets)
			last = markets
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sample feeds the report every interval. A sample can take a second or two (Docker
// measures CPU over time), so the loop never runs two at once.
func sample(ctx context.Context, s *monitor.Sampler, rec *report.Recorder, sims []*sim, every time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			markets := map[string]int{}
			for _, sm := range sims {
				markets[sm.cfg.Name] = sm.feed.ActiveMarkets()
			}
			if err := rec.Sample(time.Now(), s.Sample(ctx), markets); err != nil {
				log.Warn("sample not written", "err", err)
			}
		}
	}
}
