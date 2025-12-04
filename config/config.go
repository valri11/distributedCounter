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
	Usage        UsageConfig
}

type UsageConfig struct {
	Enabled        bool
	Type           string
	URL            string
	DelayReportSec int
}

type UsageServerConfig struct {
	ServerConfig `mapstructure:",squash"`
	UsageDB      string
}

type Configuration struct {
	ResourceServer ResourceServerConfig
	UsageServer    UsageServerConfig
}
