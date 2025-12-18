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
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/justinas/alice"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/valri11/go-servicepack/middleware/cors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/valri11/distributedcounter/config"
	"github.com/valri11/distributedcounter/metrics"
	"github.com/valri11/distributedcounter/telemetry"
	"github.com/valri11/distributedcounter/usage"
)

// resourceServerCmd represents the resourceserver command
var resourceServerCmd = &cobra.Command{
	Use:   "resourceserver",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: doResourceServerCmd,
}

func init() {
	rootCmd.AddCommand(resourceServerCmd)

	resourceServerCmd.Flags().Int("port", 8080, "service port to listen")
	resourceServerCmd.Flags().BoolP("disable-tls", "", false, "development mode (http on loclahost)")
	resourceServerCmd.Flags().String("tls-cert", "", "TLS certificate file")
	resourceServerCmd.Flags().String("tls-cert-key", "", "TLS certificate key file")
	resourceServerCmd.Flags().BoolP("disable-telemetry", "", false, "disable telemetry publishing")
	resourceServerCmd.Flags().String("telemetry-collector", "", "open telemetry grpc collector")
	resourceServerCmd.Flags().String("region", "", "service region")

	viper.BindEnv("resourceserver.disabletelemetry", "OTEL_SDK_DISABLED")
	viper.BindEnv("resourceserver.telemetrycollector", "OTEL_EXPORTER_OTLP_ENDPOINT")

	viper.BindPFlag("resourceserver.port", resourceServerCmd.Flags().Lookup("port"))
	viper.BindPFlag("resourceserver.disabletls", resourceServerCmd.Flags().Lookup("disable-tls"))
	viper.BindPFlag("resourceserver.tlscertfile", resourceServerCmd.Flags().Lookup("tls-cert"))
	viper.BindPFlag("resourceserver.tlscertkeyfile", resourceServerCmd.Flags().Lookup("tls-cert-key"))
	viper.BindPFlag("resourceserver.disablelemetry", resourceServerCmd.Flags().Lookup("disable-telemetry"))
	viper.BindPFlag("resourceserver.telemetrycollector", resourceServerCmd.Flags().Lookup("telemetry-collector"))
	viper.BindPFlag("resourceserver.resourceserver.region", resourceServerCmd.Flags().Lookup("region"))

	viper.AutomaticEnv()
}

type UsageReporter interface {
	ReportUsage(ctx context.Context, region string, accountID string, resourceUsage int64) error
}

type resourceSrvHandler struct {
	cfg     config.Configuration
	tracer  trace.Tracer
	metrics *metrics.AppMetrics

	usageReporter UsageReporter

	accounts []string
}

func doResourceServerCmd(cmd *cobra.Command, args []string) {
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
		cfg.ResourceServer.DisableTelemetry,
		cfg.ResourceServer.ServiceName,
		cfg.ResourceServer.TelemetryCollector)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := shutdown(context.Background()); err != nil {
			log.Fatal("failed to shutdown TracerProvider: %w", err)
		}
	}()

	h, err := newResourceSrvHandler(cfg)
	if err != nil {
		log.Fatalf("ERR: %v", err)
		return
	}

	mux := http.NewServeMux()

	mwChain := []alice.Constructor{
		cors.CORS,
		telemetry.WithOtelTracerContext(h.tracer),
		//telemetry.WithRequestLog(),
		metrics.WithMetrics(h.metrics),
	}
	handlerChain := alice.New(mwChain...).Then

	mux.Handle("/livez",
		handlerChain(
			otelhttp.NewHandler(http.HandlerFunc(h.livezHandler), "livez")))

	// start server listen with error handling
	srv := &http.Server{
		Addr:         fmt.Sprintf("0.0.0.0:%d", cfg.ResourceServer.Port),
		BaseContext:  func(_ net.Listener) context.Context { return ctx },
		Handler:      mux,
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	srvErr := make(chan error, 1)
	go func() {
		slog.Info("server started", "port", cfg.ResourceServer.Port)
		if cfg.ResourceServer.DisableTLS {
			srvErr <- srv.ListenAndServe()
		} else {
			srvErr <- srv.ListenAndServeTLS(cfg.ResourceServer.TLSCertFile, cfg.ResourceServer.TLSCertKeyFile)
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

func newResourceSrvHandler(cfg config.Configuration) (*resourceSrvHandler, error) {
	tracer := otel.Tracer(cfg.ResourceServer.ServiceName)

	metrics, err := metrics.NewAppMetrics(otel.GetMeterProvider().Meter(cfg.ResourceServer.ServiceName))
	if err != nil {
		return nil, err
	}

	usageReporter, err := usage.NewUsageReporter(
		cfg.ResourceServer.Usage.Type,
		cfg.ResourceServer.Usage.URL,
		usage.WithDelayReport(time.Duration(cfg.ResourceServer.Usage.DelayReportSec)*time.Second),
		usage.WithPublisherParams(cfg.ResourceServer.Usage.Options),
	)
	if err != nil {
		return nil, err
	}

	srv := resourceSrvHandler{
		cfg:           cfg,
		tracer:        tracer,
		metrics:       metrics,
		usageReporter: usageReporter,
	}

	// generate account ids
	for idx := range cfg.ResourceServer.NumAccounts {
		accountID := fmt.Sprintf("ACC%05d", idx+1)
		srv.accounts = append(srv.accounts, accountID)
	}

	return &srv, nil
}

func (h *resourceSrvHandler) livezHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tracer := telemetry.MustTracerFromContext(ctx)
	_, span := tracer.Start(ctx, "livezHandler")
	defer span.End()

	//slog.DebugContext(ctx, "livez")
	//slog.InfoContext(ctx, "test log message")

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

	if h.cfg.ResourceServer.Usage.Enabled {
		err = h.usageReporter.ReportUsage(ctx,
			h.cfg.ResourceServer.Region, h.getAccountID(), 1)
		if err != nil {
			slog.Error("report usage", "error", err)
		}
	}
}

func (h *resourceSrvHandler) getAccountID() string {
	if len(h.accounts) == 0 {
		return "123"
	}

	idx := rand.Intn(len(h.accounts))
	return h.accounts[idx]
}
