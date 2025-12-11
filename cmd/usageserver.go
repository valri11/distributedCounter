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
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/jmoiron/sqlx"
	"github.com/justinas/alice"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"

	leaderelection "github.com/drio-ai/leaderelection"
	leaderElectionWithRedis "github.com/drio-ai/leaderelection/redis"
	"github.com/golang/groupcache/consistenthash"
	"github.com/valri11/distributedcounter/config"
	"github.com/valri11/distributedcounter/metrics"
	"github.com/valri11/distributedcounter/subscriber"
	"github.com/valri11/distributedcounter/telemetry"
	"github.com/valri11/distributedcounter/types"
	"github.com/valri11/distributedcounter/usage"
	"github.com/valri11/go-servicepack/middleware/cors"
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
	usageServerCmd.Flags().Int("gossip-port", 8070, "gossip port to listen for peers")
	usageServerCmd.Flags().BoolP("disable-tls", "", false, "development mode (http on loclahost)")
	usageServerCmd.Flags().String("tls-cert", "", "TLS certificate file")
	usageServerCmd.Flags().String("tls-cert-key", "", "TLS certificate key file")
	usageServerCmd.Flags().BoolP("disable-telemetry", "", false, "disable telemetry publishing")
	usageServerCmd.Flags().String("telemetry-collector", "", "open telemetry grpc collector")
	usageServerCmd.Flags().String("usage-db", "", "usage DB connection string")
	usageServerCmd.Flags().String("broker-queue", "", "message broker queue name to create")

	viper.BindEnv("usageserver.disabletelemetry", "OTEL_SDK_DISABLED")
	viper.BindEnv("usageserver.telemetrycollector", "OTEL_EXPORTER_OTLP_ENDPOINT")

	viper.BindPFlag("usageserver.port", usageServerCmd.Flags().Lookup("port"))
	viper.BindPFlag("usageserver.disabletls", usageServerCmd.Flags().Lookup("disable-tls"))
	viper.BindPFlag("usageserver.tlscertfile", usageServerCmd.Flags().Lookup("tls-cert"))
	viper.BindPFlag("usageserver.tlscertkeyfile", usageServerCmd.Flags().Lookup("tls-cert-key"))
	viper.BindPFlag("usageserver.disablelemetry", usageServerCmd.Flags().Lookup("disable-telemetry"))
	viper.BindPFlag("usageserver.telemetrycollector", usageServerCmd.Flags().Lookup("telemetry-collector"))
	viper.BindPFlag("usageserver.usagedb", usageServerCmd.Flags().Lookup("usage-db"))
	viper.BindPFlag("usageserver.msgsubscription.queue", usageServerCmd.Flags().Lookup("broker-queue"))
	viper.BindPFlag("usageserver.peerdiscovery.gossipport", usageServerCmd.Flags().Lookup("gossip-port"))

	viper.AutomaticEnv()
}

type ServiceNode struct {
	Name            string
	IsLeader        bool
	QueueAssignment map[string][]string
}

func NewServiceNode(name string, isLeader bool) ServiceNode {
	return ServiceNode{
		Name:            name,
		IsLeader:        isLeader,
		QueueAssignment: make(map[string][]string),
	}
}

type usageSrvHandler struct {
	cfg     config.Configuration
	tracer  trace.Tracer
	metrics *metrics.AppMetrics

	db              *sqlx.DB
	resourceManager *usage.UsageManager
	memeberList     *memberlist.Memberlist
	nodeName        string
	serviceNodes    map[string]ServiceNode
	mxNodes         *sync.RWMutex
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

	ctx := context.Background()

	brokerURL, err := url.Parse(cfg.UsageServer.MsgSubscription.URL)
	if err != nil {
		return nil, fmt.Errorf("Error parsing URL: %v\n", err)
	}

	// Create a UserInfo object with the username and password
	brokerURL.User = url.UserPassword(cfg.UsageServer.MsgSubscription.User, cfg.UsageServer.MsgSubscription.Password)

	msgProvider, err := subscriber.NewAMQPMessageProvider(
		ctx,
		brokerURL.String(),
		cfg.UsageServer.MsgSubscription.VHost,
		subscriber.DefaultDialer,
	)
	if err != nil {
		return nil, fmt.Errorf("create event watcher: %w", err)
	}

	resourceManager, err := usage.NewUsageManager(store)
	if err != nil {
		return nil, err
	}

	msgSubscriberCfg := subscriber.NewMessageSubscriberConfig(
		cfg.UsageServer.MsgSubscription.ExchangeName,
		fmt.Sprintf("%s%02d", cfg.UsageServer.MsgSubscription.Queue, 1),
		nil,
		resourceManager.ProcessMessage,
	)

	err = msgProvider.Subscribe(ctx, msgSubscriberCfg)
	if err != nil {
		return nil, fmt.Errorf("subscribe AM processor: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.UsageServer.LeaderElection.URL,
		Password: "",
		DB:       0,
	})

	// Ping the Redis server to check the connection
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		slog.Error("error connecting to redis:", "error", err)
	} else {
		slog.Info("redis connection successful:", "ping", pong)
	}

	memeberListConfig := memberlist.DefaultLocalConfig()

	hostname, _ := os.Hostname()
	nodeName := fmt.Sprintf("%s:%d", hostname, cfg.UsageServer.PeerDiscovery.GossipPort)
	memeberListConfig.Name = nodeName

	memeberListConfig.BindPort = cfg.UsageServer.PeerDiscovery.GossipPort
	//memeberListConfig.BindAddr = cfg.UsageServer.PeerDiscovery.GossipBindAddr

	//memeberListConfig.AdvertiseAddr = memeberListConfig.BindAddr
	//memeberListConfig.AdvertisePort = memeberListConfig.BindPort

	/*
		_, ipNet, err := net.ParseCIDR("0.0.0.0/0")
		if err != nil {
			// handle error
		}
		memeberListConfig.CIDRsAllowed = []net.IPNet{
			*ipNet,
		}
	*/

	memeberList, err := memberlist.Create(memeberListConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	srv := usageSrvHandler{
		cfg:             cfg,
		tracer:          tracer,
		metrics:         metrics,
		db:              db,
		resourceManager: resourceManager,
		memeberList:     memeberList,
		mxNodes:         &sync.RWMutex{},
		nodeName:        nodeName,
		serviceNodes:    make(map[string]ServiceNode),
	}

	srv.serviceNodes[nodeName] = NewServiceNode(nodeName, false)

	memeberListConfig.Events = &srv
	memeberListConfig.Delegate = &srv

	var seedPeers []string
	for _, sp := range cfg.UsageServer.PeerDiscovery.SeedPeers {
		seedPeers = append(seedPeers, sp.URL)
	}
	_, err = memeberList.Join(seedPeers)
	if err != nil {
		return nil, fmt.Errorf("failed to join memberlist: %w", err)
	}

	leaderElectionCfg := leaderElectionWithRedis.RedisLeaderElectionConfig{
		LeaderElectionConfig: leaderelection.LeaderElectionConfig{
			RelinquishInterval:    30 * time.Second,
			LeaderCheckInterval:   10 * time.Second,
			FollowerCheckInterval: 10 * time.Second,
			LeaderCallback:        srv.LeaderCallback,
			FollowerCallback:      srv.FollowerCallback,
		},
	}

	leaderElection, err := leaderElectionWithRedis.NewWithConn(ctx, rdb, leaderElectionCfg)
	if err != nil {
		panic(err)
	}

	go func(ctx context.Context) {
		leaderElection.Run(ctx)
	}(ctx)

	return &srv, nil
}

func (s *usageSrvHandler) LeaderCallback(context.Context) error {
	slog.Debug("leader callback")

	s.mxNodes.Lock()
	defer s.mxNodes.Unlock()

	node := s.serviceNodes[s.nodeName]
	node.IsLeader = true

	ch := consistenthash.New(1, nil)

	for _, m := range s.memeberList.Members() {
		ch.Add(m.Name)
	}

	node.QueueAssignment = make(map[string][]string)
	for idx := range s.memeberList.NumMembers() {
		queueName := fmt.Sprintf("%s%02d", s.cfg.UsageServer.MsgSubscription.Queue, idx+1)
		// get node for queue
		nodeName := ch.Get(queueName)

		slog.Debug("assign queue", "node", nodeName, "queue", queueName)

		node.QueueAssignment[nodeName] = append(node.QueueAssignment[nodeName], queueName)
	}

	s.serviceNodes[s.nodeName] = node

	slog.Debug("service state", "state", s.serviceNodes)

	return nil
}

func (s *usageSrvHandler) FollowerCallback(context.Context) error {
	slog.Debug("follower callback")

	s.mxNodes.Lock()
	defer s.mxNodes.Unlock()

	node := s.serviceNodes[s.nodeName]
	node.IsLeader = false
	node.QueueAssignment = nil

	s.serviceNodes[s.nodeName] = node

	return nil
}

func (s *usageSrvHandler) NotifyJoin(node *memberlist.Node) {
	slog.Debug("node joined", "node", node)
}

func (s *usageSrvHandler) NotifyLeave(node *memberlist.Node) {
	slog.Debug("node left", "node", node)

	s.mxNodes.Lock()
	defer s.mxNodes.Unlock()

	delete(s.serviceNodes, node.Name)
}

func (s *usageSrvHandler) NotifyUpdate(node *memberlist.Node) {
	slog.Debug("node updated", "node", node)
}

func (s *usageSrvHandler) NodeMeta(limit int) []byte {
	return []byte(fmt.Sprintf("node service port: %d", s.cfg.UsageServer.Port))
}

func (s *usageSrvHandler) NotifyMsg(msg []byte) {
	fmt.Printf("got message: %v\n", string(msg))
}

func (s *usageSrvHandler) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (s *usageSrvHandler) LocalState(join bool) []byte {
	s.mxNodes.RLock()
	defer s.mxNodes.RUnlock()

	msg, err := json.Marshal(s.serviceNodes[s.nodeName])
	if err != nil {
		slog.Error("failed to marshal node state", "error", err)
		return nil
	}
	return msg
}

func (s *usageSrvHandler) MergeRemoteState(msg []byte, join bool) {
	var node ServiceNode
	err := json.Unmarshal(msg, &node)
	if err != nil {
		slog.Error("failed to unmarshal node state", "error", err)
		return
	}

	s.mxNodes.Lock()
	defer s.mxNodes.Unlock()

	if node.Name != s.nodeName {
		s.serviceNodes[node.Name] = node

		slog.Debug("service state", "state", s.serviceNodes)
	}
}

func (h *usageSrvHandler) setUsageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	var resourceUsage []types.AccountUsage
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

	for _, usage := range resourceUsage {
		h.resourceManager.RecordUsage(ctx,
			usage.Region,
			usage.AccountID,
			usage.TS,
			usage.Counter)
	}

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
