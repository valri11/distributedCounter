package config

type ServerConfig struct {
	ServiceName        string
	Port               int
	DisableTLS         bool
	TLSCertFile        string
	TLSCertKeyFile     string
	DisableTelemetry   bool
	TelemetryCollector string
}

type ResourceServerConfig struct {
	ServerConfig `mapstructure:",squash"`
	Region       string
	NumAccounts  int
	Usage        UsageConfig
}

type UsageConfig struct {
	Enabled        bool
	Type           string
	URL            string
	DelayReportSec int
	Options        map[string]string
}

type MsgSubscriptionConfig struct {
	URL          string
	VHost        string
	ExchangeName string
	Queue        string
	User         string
	Password     string
}

type SeedPeerConfig struct {
	URL string
}

type PeerDiscoveryConfig struct {
	GossipBindAddr string
	GossipPort     int
	SeedPeers      []SeedPeerConfig
}

type UsageServerConfig struct {
	ServerConfig    `mapstructure:",squash"`
	UsageDB         string
	MsgSubscription MsgSubscriptionConfig
	PeerDiscovery   PeerDiscoveryConfig
}

type Configuration struct {
	ResourceServer ResourceServerConfig
	UsageServer    UsageServerConfig
}
