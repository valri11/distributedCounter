/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/justinas/alice"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	"github.com/valri11/distributedcounter/config"
	"github.com/valri11/distributedcounter/metrics"
	"github.com/valri11/distributedcounter/telemetry"
	"github.com/valri11/distributedcounter/usage"
	"github.com/valri11/go-servicepack/middleware/cors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
)

// usageserverCmd represents the usageserver command
var usageServerCmd = &cobra.Command{
	Use:   "usageserver",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: doUsageServerCmd,
}

func init() {
	rootCmd.AddCommand(usageServerCmd)

	usageServerCmd.Flags().Int("port", 8080, "service port to listen")
	usageServerCmd.Flags().BoolP("disable-tls", "", false, "development mode (http on loclahost)")
	usageServerCmd.Flags().String("tls-cert", "", "TLS certificate file")
	usageServerCmd.Flags().String("tls-cert-key", "", "TLS certificate key file")
	usageServerCmd.Flags().BoolP("disable-telemetry", "", false, "disable telemetry publishing")
	usageServerCmd.Flags().String("telemetry-collector", "", "open telemetry grpc collector")
	usageServerCmd.Flags().String("usage-db", "", "usage DB connection string")

	viper.BindEnv("usageserver.disabletelemetry", "OTEL_SDK_DISABLED")
	viper.BindEnv("usageserver.telemetrycollector", "OTEL_EXPORTER_OTLP_ENDPOINT")

	viper.BindPFlag("usageserver.port", usageServerCmd.Flags().Lookup("port"))
	viper.BindPFlag("usageserver.disabletls", usageServerCmd.Flags().Lookup("disable-tls"))
	viper.BindPFlag("usageserver.tlscertfile", usageServerCmd.Flags().Lookup("tls-cert"))
	viper.BindPFlag("usageserver.tlscertkeyfile", usageServerCmd.Flags().Lookup("tls-cert-key"))
	viper.BindPFlag("usageserver.disablelemetry", usageServerCmd.Flags().Lookup("disable-telemetry"))
	viper.BindPFlag("usageserver.telemetrycollector", usageServerCmd.Flags().Lookup("telemetry-collector"))
	viper.BindPFlag("usageserver.usagedb", usageServerCmd.Flags().Lookup("usage-db"))

	viper.AutomaticEnv()
}

type usageSrvHandler struct {
	cfg     config.Configuration
	tracer  trace.Tracer
	metrics *metrics.AppMetrics

	db              *sqlx.DB
	resourceManager *usage.UsageManager
}

func doUsageServerCmd(cmd *cobra.Command, args []string) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	var cfg config.Configuration
	err := viper.Unmarshal(&cfg)
	if err != nil {
		log.Fatalf("ERR: %v", err)
		return
	}
	slog.Debug("config", "cfg", cfg)

	ctx := context.Background()
	// Handle SIGINT (CTRL+C) gracefully.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	shutdown, err := telemetry.InitProviders(context.Background(),
		cfg.UsageServer.DisableTelemetry,
		cfg.UsageServer.ServiceName,
		cfg.UsageServer.TelemetryCollector)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := shutdown(context.Background()); err != nil {
			log.Fatal("failed to shutdown TracerProvider: %w", err)
		}
	}()

	h, err := newUsageSrvHandler(cfg)
	if err != nil {
		log.Fatalf("ERR: %v", err)
		return
	}

	mux := http.NewServeMux()

	mwChain := []alice.Constructor{
		cors.CORS,
		telemetry.WithOtelTracerContext(h.tracer),
		telemetry.WithRequestLog(),
		metrics.WithMetrics(h.metrics),
	}
	handlerChain := alice.New(mwChain...).Then

	mux.Handle("/usage/set",
		handlerChain(
			otelhttp.NewHandler(http.HandlerFunc(h.setUsageHandler),
				"usage_set")))
	mux.Handle("/usage/report",
		handlerChain(
			otelhttp.NewHandler(http.HandlerFunc(h.reportUsageHandler),
				"usage_report")))

	// start server listen with error handling
	srv := &http.Server{
		Addr:         fmt.Sprintf("0.0.0.0:%d", cfg.UsageServer.Port),
		BaseContext:  func(_ net.Listener) context.Context { return ctx },
		Handler:      mux,
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("server started", "port", cfg.UsageServer.Port)
		if cfg.UsageServer.DisableTLS {
			srvErr <- srv.ListenAndServe()
		} else {
			srvErr <- srv.ListenAndServeTLS(cfg.UsageServer.TLSCertFile, cfg.UsageServer.TLSCertKeyFile)
		}
	}()

	// Wait for interruption.
	select {
	case <-srvErr:
		// Error when starting HTTP server.
		return
	case <-ctx.Done():
		// Wait for first CTRL+C.
		// Stop receiving signal notifications as soon as possible.
		stop()
	}

	// When Shutdown is called, ListenAndServe immediately returns ErrServerClosed.
	srv.Shutdown(context.Background())
}

func newUsageSrvHandler(cfg config.Configuration) (*usageSrvHandler, error) {
	tracer := otel.Tracer(cfg.UsageServer.ServiceName)

	metrics, err := metrics.NewAppMetrics(otel.GetMeterProvider().Meter(cfg.UsageServer.ServiceName))
	if err != nil {
		return nil, err
	}

	db, err := otelsqlx.Open("postgres", cfg.UsageServer.UsageDB, otelsql.WithAttributes(semconv.DBSystemPostgreSQL))
	if err != nil {
		return nil, err
	}

	store, err := usage.NewStore(db)
	if err != nil {
		return nil, err
	}

	resourceManager, err := usage.NewUsageManager(store)
	if err != nil {
		return nil, err
	}

	srv := usageSrvHandler{
		cfg:             cfg,
		tracer:          tracer,
		metrics:         metrics,
		db:              db,
		resourceManager: resourceManager,
	}

	return &srv, nil
}

func (h *usageSrvHandler) setUsageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	var resourceUsage usage.AccountUsage
	// Create a new JSON decoder for the request body
	decoder := json.NewDecoder(r.Body)

	// Decode the JSON from the request body into the User struct
	err := decoder.Decode(&resourceUsage)
	if err != nil {
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}

	// Close the request body after decoding
	defer r.Body.Close()

	slog.DebugContext(ctx, "set usage", "resourceUsage", resourceUsage)

	h.resourceManager.RecordUsage(ctx, resourceUsage.AccountID, resourceUsage.Counter)

	tracer := telemetry.MustTracerFromContext(ctx)
	_, span := tracer.Start(ctx, "setUsageHandler")
	defer span.End()

	slog.DebugContext(ctx, "set usage")

	res := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}

	out, err := json.Marshal(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}

func (h *usageSrvHandler) reportUsageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	slog.DebugContext(ctx, "report usage")

	usageInfo, err := h.resourceManager.UsageInfo(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "usage info", "error", err)
		http.Error(w, "get usage info", http.StatusBadRequest)
		return
	}

	res := usageInfo

	out, err := json.Marshal(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}
