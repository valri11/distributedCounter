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
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/jmoiron/sqlx"
	"github.com/justinas/alice"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/valri11/distributedcounter/config"
	"github.com/valri11/distributedcounter/election"
	"github.com/valri11/distributedcounter/metrics"
	"github.com/valri11/distributedcounter/queuemanager"
	rabbitmqclient "github.com/valri11/distributedcounter/rabbitmq/client"
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
	memberList      *memberlist.Memberlist
	nodeName        string
	serviceNodes    map[string]ServiceNode
	mxNodes         *sync.RWMutex

	electionManager *election.ElectionManager
	queueManager    subscriber.QueueManager
	messageProvider subscriber.MessageProvider
}

func doUsageServerCmd(cmd *cobra.Command, args []string) {
	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
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

	resourceManager, err := usage.NewUsageManager(store)
	if err != nil {
		return nil, err
	}

	hostname, _ := os.Hostname()
	nodeName := fmt.Sprintf("%s:%d", hostname, cfg.UsageServer.PeerDiscovery.GossipPort)

	var msgProvider subscriber.MessageProvider
	var queueManager subscriber.QueueManager

	switch cfg.UsageServer.MsgSubscription.Type {
	case "amqp":
		brokerURL, err := url.Parse(cfg.UsageServer.MsgSubscription.URL)
		if err != nil {
			return nil, fmt.Errorf("error parsing URL: %v\n", err)
		}

		// Create a UserInfo object with the username and password
		brokerURL.User = url.UserPassword(cfg.UsageServer.MsgSubscription.User, cfg.UsageServer.MsgSubscription.Password)

		msgProvider, err = subscriber.NewAMQPMessageProvider(
			ctx,
			brokerURL.String(),
			cfg.UsageServer.MsgSubscription.VHost,
			subscriber.DefaultDialer,
		)
		if err != nil {
			return nil, fmt.Errorf("create amqp message provider: %w", err)
		}

		amqpClient, err := rabbitmqclient.NewClient(
			cfg.UsageServer.MsgSubscription.QueueManager.URL,
			cfg.UsageServer.MsgSubscription.QueueManager.User,
			cfg.UsageServer.MsgSubscription.QueueManager.Password,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create rabbitmq client: %w", err)
		}

		queueManager, err = queuemanager.NewQueueManager(amqpClient,
			cfg.UsageServer.MsgSubscription.VHost,
			cfg.UsageServer.MsgSubscription.ExchangeName,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create queue manager: %w", err)
		}

		if !cfg.UsageServer.MsgSubscription.QueueManager.Enabled {
			msgSubscriberCfg := subscriber.NewMessageSubscriberConfig(
				cfg.UsageServer.MsgSubscription.ExchangeName,
				fmt.Sprintf("%s%02d", cfg.UsageServer.MsgSubscription.Queue, 1),
				nodeName,
				nil,
				resourceManager.ProcessMessage,
			)

			err = msgProvider.Subscribe(ctx, msgSubscriberCfg)
			if err != nil {
				return nil, fmt.Errorf("subscribe AM processor: %w", err)
			}
		}

	case "kafka":
		msgProvider, err = subscriber.NewKafkaMessageProvider(
			ctx,
			cfg.UsageServer.MsgSubscription.URL,
			cfg.UsageServer.MsgSubscription.Options,
			resourceManager.ProcessMessage,
		)
		if err != nil {
			return nil, fmt.Errorf("create kafka message provider: %w", err)
		}

		err = msgProvider.Subscribe(ctx, &subscriber.MsgSubscriberConfig{})
		if err != nil {
			return nil, fmt.Errorf("subscribe AM processor: %w", err)
		}

	default:
		return nil, fmt.Errorf("unknown message provider: %s", cfg.UsageServer.MsgSubscription.Type)
	}

	srv := usageSrvHandler{
		cfg:             cfg,
		tracer:          tracer,
		metrics:         metrics,
		db:              db,
		resourceManager: resourceManager,
		mxNodes:         &sync.RWMutex{},
		nodeName:        nodeName,
		serviceNodes:    make(map[string]ServiceNode),
		queueManager:    queueManager,
		messageProvider: msgProvider,
	}

	em, err := election.New(ctx,
		cfg.UsageServer.LeaderElection,
		srv.LeaderCallback,
		srv.FollowerCallback,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create election manager: %w", err)
	}
	srv.electionManager = em

	em.Run(ctx)

	if cfg.UsageServer.PeerDiscovery.Enabled {
		memberListConfig := memberlist.DefaultLocalConfig()

		memberListConfig.Name = nodeName
		memberListConfig.BindPort = cfg.UsageServer.PeerDiscovery.GossipPort
		if cfg.UsageServer.PeerDiscovery.GossipBindAddr != "" {
			memberListConfig.BindAddr = cfg.UsageServer.PeerDiscovery.GossipBindAddr
		}

		memberList, err := memberlist.Create(memberListConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create memberlist: %w", err)
		}

		srv.serviceNodes[nodeName] = NewServiceNode(nodeName, false)

		memberListConfig.Events = &srv
		memberListConfig.Delegate = &srv

		var seedPeers []string
		for _, sp := range cfg.UsageServer.PeerDiscovery.SeedPeers {
			seedPeers = append(seedPeers, sp.URL)
		}
		_, err = memberList.Join(seedPeers)
		if err != nil {
			return nil, fmt.Errorf("failed to join memberlist: %w", err)
		}

		srv.memberList = memberList
	}

	return &srv, nil
}

func (s *usageSrvHandler) processQueueAssignment(assignedQueues []string) {
	currentSubscribedQueues := s.messageProvider.Status()

	toRemove := maps.Clone(currentSubscribedQueues)
	toAdd := make(map[string]bool)

	for _, q := range assignedQueues {
		if ok := toRemove[q]; ok {
			delete(toRemove, q)
		} else {
			toAdd[q] = true
		}
	}

	if len(toAdd) == 0 && len(toRemove) == 0 {
		slog.Debug("process queue assignment - no changes")
		return
	}

	slog.Debug("process queue assignment", "add", toAdd, "remove", toRemove)

	ctx := context.Background()

	for queueName := range toAdd {
		msgSubscriberCfg := subscriber.NewMessageSubscriberConfig(
			s.cfg.UsageServer.MsgSubscription.ExchangeName,
			queueName,
			s.nodeName,
			nil,
			s.resourceManager.ProcessMessage,
		)

		err := s.messageProvider.Subscribe(ctx, msgSubscriberCfg)
		if err != nil {
			slog.Error("error subscribe AM processor", "error", err)
		}
	}

	for queueName := range toRemove {
		err := s.messageProvider.Unsubscribe(ctx, queueName)
		if err != nil {
			slog.Error("error unsubscribe AM processor", "error", err)
		}
	}
}

func (s *usageSrvHandler) LeaderCallback(ctx context.Context) error {
	if !s.cfg.UsageServer.LeaderElection.Enabled {
		return nil
	}

	slog.Debug("leader callback")

	s.mxNodes.Lock()
	defer s.mxNodes.Unlock()

	node := s.serviceNodes[s.nodeName]
	node.IsLeader = true

	/*
		ch := consistenthash.New(50, nil)
		for _, m := range s.memberList.Members() {
			ch.Add(m.Name)
		}
	*/
	cd := queuemanager.NewConsistentDistributor()
	for _, m := range s.memberList.Members() {
		cd.Add(m.Name)
	}

	existingQueues, err := s.queueManager.GetExchangeQueues(ctx)
	if err != nil {
		return err
	}

	queueMap := make(map[string]bool)
	for _, queueName := range existingQueues {
		queueMap[queueName] = true
	}

	// if reaquired create additional queues
	if s.cfg.UsageServer.MsgSubscription.QueueManager.MaxQueuesNum == 0 {
		s.cfg.UsageServer.MsgSubscription.QueueManager.MaxQueuesNum = math.MaxInt
	}
	maxQueuesNum := min(s.memberList.NumMembers(), s.cfg.UsageServer.MsgSubscription.QueueManager.MaxQueuesNum)
	for idx := range maxQueuesNum {
		queueName := fmt.Sprintf("%s%02d", s.cfg.UsageServer.MsgSubscription.Queue, idx+1)
		queueMap[queueName] = true
	}

	// list of queues
	var queues []string
	for q := range queueMap {
		queues = append(queues, q)
	}
	slices.Sort(queues)

	// each node will get at least one queue

	node.QueueAssignment = make(map[string][]string)

	queueToNodes := make(map[string][]string)
	nodeToQueues := make(map[string][]string)

	for _, queueName := range queues {
		// get node for queue
		//nodeName := ch.Get(queueName)
		nodeName := cd.Get(queueName)

		slog.Debug("assign queue", "node", nodeName, "queue", queueName)

		nodeToQueues[nodeName] = append(nodeToQueues[nodeName], queueName)
		queueToNodes[queueName] = append(queueToNodes[queueName], nodeName)
	}

	if s.memberList.NumMembers() > len(queues) {
		// find nodes without queue assigned
		var nodesWithoutQueue []string
		for _, m := range s.memberList.Members() {
			if ql, ok := nodeToQueues[m.Name]; ok {
				if len(ql) == 0 {
					nodesWithoutQueue = append(nodesWithoutQueue, m.Name)
				}
			} else {
				nodesWithoutQueue = append(nodesWithoutQueue, m.Name)
			}
		}
		slices.Sort(nodesWithoutQueue)

		queueIdx := 0
		for _, nodeName := range nodesWithoutQueue {
			if queueIdx == len(queues) {
				queueIdx = 0
			}
			queueName := queues[queueIdx]
			queueIdx++

			nodeToQueues[nodeName] = append(nodeToQueues[nodeName], queueName)
			queueToNodes[queueName] = append(queueToNodes[queueName], nodeName)
		}
	}

	// sort and remove dups
	for _, m := range s.memberList.Members() {
		queues := nodeToQueues[m.Name]
		slices.Sort(queues)
		queues = slices.Compact(queues)
		nodeToQueues[m.Name] = queues

	}
	node.QueueAssignment = nodeToQueues

	s.serviceNodes[s.nodeName] = node

	slog.Debug("service state", "state", s.serviceNodes)

	// process possible subscription queue change
	for _, nd := range s.serviceNodes {
		if nd.IsLeader {
			s.processQueueAssignment(nd.QueueAssignment[s.nodeName])
			break
		}
	}

	return nil
}

func (s *usageSrvHandler) FollowerCallback(context.Context) error {
	if !s.cfg.UsageServer.LeaderElection.Enabled {
		return nil
	}

	slog.Debug("follower callback")

	s.mxNodes.Lock()
	defer s.mxNodes.Unlock()

	node := s.serviceNodes[s.nodeName]
	node.IsLeader = false
	node.QueueAssignment = nil

	s.serviceNodes[s.nodeName] = node

	// process possible subscription queue change
	for _, nd := range s.serviceNodes {
		if nd.IsLeader {
			s.processQueueAssignment(nd.QueueAssignment[s.nodeName])
			break
		}
	}

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

	//slog.DebugContext(ctx, "set usage", "resourceUsage", resourceUsage)

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

	//slog.DebugContext(ctx, "set usage")

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
