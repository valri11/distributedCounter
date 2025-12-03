/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bytes"
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

	viper.BindEnv("resourceserver.disabletelemetry", "OTEL_SDK_DISABLED")
	viper.BindEnv("resourceserver.telemetrycollector", "OTEL_EXPORTER_OTLP_ENDPOINT")

	viper.BindPFlag("resourceserver.port", resourceServerCmd.Flags().Lookup("port"))
	viper.BindPFlag("resourceserver.disabletls", resourceServerCmd.Flags().Lookup("disable-tls"))
	viper.BindPFlag("resourceserver.tlscertfile", resourceServerCmd.Flags().Lookup("tls-cert"))
	viper.BindPFlag("resourceserver.tlscertkeyfile", resourceServerCmd.Flags().Lookup("tls-cert-key"))
	viper.BindPFlag("resourceserver.disablelemetry", resourceServerCmd.Flags().Lookup("disable-telemetry"))
	viper.BindPFlag("resourceserver.telemetrycollector", resourceServerCmd.Flags().Lookup("telemetry-collector"))

	viper.AutomaticEnv()
}

type resourceSrvHandler struct {
	cfg     config.Configuration
	tracer  trace.Tracer
	metrics *metrics.AppMetrics
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
		telemetry.WithRequestLog(),
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
		panic(err)
	}

	srv := resourceSrvHandler{
		cfg:     cfg,
		tracer:  tracer,
		metrics: metrics,
	}

	return &srv, nil
}

func (h *resourceSrvHandler) livezHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tracer := telemetry.MustTracerFromContext(ctx)
	_, span := tracer.Start(ctx, "livezHandler")
	defer span.End()

	slog.DebugContext(ctx, "livez")
	slog.InfoContext(ctx, "test log message")

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
		fmt.Printf("reporting usage: %s url: %s\n",
			h.cfg.ResourceServer.Usage.Type,
			h.cfg.ResourceServer.Usage.URL,
		)

		resUsage := AccountUsage{
			AccountID: "123",
			Counter:   1,
		}

		// Marshal the user data into JSON
		jsonData, err := json.Marshal(resUsage)
		if err != nil {
			log.Fatalf("Error marshaling JSON: %v", err)
		}

		// Create a new HTTP client
		client := &http.Client{}

		req, err := http.NewRequest(http.MethodPost,
			h.cfg.ResourceServer.Usage.URL,
			bytes.NewBuffer(jsonData))
		if err != nil {
			log.Fatalf("Error creating request: %v", err)
		}

		// Set the Content-Type header to application/json
		req.Header.Set("Content-Type", "application/json")

		// Send the request
		resp, err := client.Do(req)
		if err != nil {
			log.Fatalf("Error sending request: %v", err)
		}
		defer resp.Body.Close()

		// Check the response status code
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
			fmt.Printf("POST request successful. Status: %s\n", resp.Status)
		} else {
			fmt.Printf("POST request failed. Status: %s\n", resp.Status)
		}

	}
}
