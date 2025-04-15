package config

import (
	"fmt"

	"github.com/fsnotify/fsnotify" // For watching config file changes
	"github.com/spf13/viper"
)

// Config represents the combined framework and service configurations.
type Config struct {
	Framework FrameworkConfig       `mapstructure:"framework"`
	Service   map[string]interface{} `mapstructure:"service"` // Holds service-specific configs
	// Compliance configuration
	ComplianceEnabled bool   `mapstructure:"compliance_enabled"`
	AppName          string `mapstructure:"app_name"`
	Env              string `mapstructure:"env"`
	PCIEnabled       bool   `mapstructure:"pci_enabled"`
	EncryptionKey    string `mapstructure:"encryption_key"`
}

// FrameworkConfig contains settings for the core framework.
type FrameworkConfig struct {
	ServiceName       string            `mapstructure:"service_name"`
	Version           string            `mapstructure:"version"`
	RPC               RPCConfig         `mapstructure:"rpc"`
	Transport         TransportConfig   `mapstructure:"transport"`
	Logging           LoggingConfig     `mapstructure:"logging"`
	Observability     ObservabilityConfig `mapstructure:"observability"`
	Security          SecurityConfig    `mapstructure:"security"`
	EnableDynamicReload bool            `mapstructure:"enable_dynamic_reload"`
}

// RPCConfig contains settings for handling RPC responses.
type RPCConfig struct {
	ResponseStreamType string `mapstructure:"response_stream_type"` // "single" or "dedicated"
	MaxRetries         int    `mapstructure:"max_retries"`           // Retry failed RPC calls
	TimeoutMs          int    `mapstructure:"timeout_ms"`            // Default timeout for RPC calls
}

// TransportConfig contains messaging transport settings.
type TransportConfig struct {
	BrokerType     string `mapstructure:"broker_type"` // "redis", "nats", "kafka" (future extensibility)
	RedisAddress   string `mapstructure:"redis_address"`
	RedisCache     bool   `mapstructure:"redis_cache"`
	UseTrimExact   bool   `mapstructure:"use_trim_exact"`
	UseCluster     bool   `mapstructure:"use_cluster"`
	PoolCount      int    `mapstructure:"pool_count"`
	WaitTimeMs     int    `mapstructure:"wait_time_ms"`
	UseEncryption  bool   `mapstructure:"use_encryption"`
	UseCompression bool   `mapstructure:"use_compression"`
	TLSRequired    bool   `mapstructure:"tls_required"` // Enforce TLS connections
}

// LoggingConfig contains logging-related settings.
type LoggingConfig struct {
	Level           string `mapstructure:"level"`
	Format          string `mapstructure:"format"` // "json" or "text"
	RedactSensitive bool   `mapstructure:"redact_sensitive"`
	DebugEnabled    bool   `mapstructure:"debug_enabled"` // Toggle verbose debugging logs
}

// ObservabilityConfig contains observability settings.
type ObservabilityConfig struct {
	TracingEnabled   bool   `mapstructure:"tracing_enabled"` // OpenTelemetry support
	OpentracingHost  string `mapstructure:"opentracing_host"`
	OpentracingPort  int    `mapstructure:"opentracing_port"`
	MetricsAdapter   string `mapstructure:"metrics_adapter"`
	MetricsHost      string `mapstructure:"metrics_host"`
	MetricsPort      int    `mapstructure:"metrics_port"`
	LogLevelMetrics  bool   `mapstructure:"log_level_metrics"` // Toggle log-based metrics
}

// SecurityConfig contains security-related settings.
type SecurityConfig struct {
	EnableJWT        bool   `mapstructure:"enable_jwt"`
	JWTSigningMethod string `mapstructure:"jwt_signing_method"` // "HS256", "RS256", etc.
	EnableRBAC       bool   `mapstructure:"enable_rbac"`
	TLSStrict        bool   `mapstructure:"tls_strict"` // Enforce strict TLS connections
}

// LoadConfig loads both the framework and service-specific configurations.
func LoadConfig(frameworkConfigPath string, serviceConfigPath string) (*Config, error) {
	viper.SetConfigFile(frameworkConfigPath)
	viper.SetConfigType("yaml")
	viper.AutomaticEnv() // Bind ENV variables
	viper.SetEnvPrefix("APP") // Set environment variable prefix (e.g., APP_SERVICE_NAME)

	// Set default values
	setDefaults()

	// Read framework config file
	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("⚠️ Warning: No framework config file found, using defaults & environment variables.")
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("❌ Failed to parse framework config: %w", err)
	}

	// Load service-specific config
	serviceViper := viper.New()
	serviceViper.SetConfigFile(serviceConfigPath)
	serviceViper.SetConfigType("yaml")

	if err := serviceViper.ReadInConfig(); err != nil {
		fmt.Println("⚠️ Warning: No service config file found, using framework defaults.")
	} else {
		if err := serviceViper.Unmarshal(&config.Service); err != nil {
			return nil, fmt.Errorf("❌ Failed to parse service config: %w", err)
		}
	}

	// Validate the loaded configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("❌ Configuration validation failed: %w", err)
	}

	// Enable dynamic reloading if configured
	if config.Framework.EnableDynamicReload {
		viper.WatchConfig()
		viper.OnConfigChange(func(e fsnotify.Event) {
			fmt.Println("⚡ Configuration file changed:", e.Name)
			if err := viper.Unmarshal(&config); err != nil {
				fmt.Println("❌ Failed to reload configuration:", err)
			} else {
				fmt.Println("✅ Configuration reloaded successfully.")
			}
		})
	} else {
		fmt.Println("🛑 Dynamic configuration reloading is disabled.")
	}

	return &config, nil
}

// Validate ensures the configuration is complete and correct.
func (c *Config) Validate() error {
	var missingFields []string

	// Check for required fields and add them to the list of missing fields
	if c.Framework.Transport.RedisAddress == "" {
		missingFields = append(missingFields, "RedisAddress")
	}
	if c.AppName == "" {
		missingFields = append(missingFields, "AppName")
	}
	if c.EncryptionKey == "" {
		missingFields = append(missingFields, "EncryptionKey")
	}

	// If there are any missing fields, return an error with all missing fields listed
	if len(missingFields) > 0 {
		return fmt.Errorf("Missing required fields: %s", missingFields)
	}

	return nil
}

// setDefaults initializes default values for framework configuration.
func setDefaults() {
	viper.SetDefault("framework.service_name", "example-service")
	viper.SetDefault("framework.version", "1.0.0")
	viper.SetDefault("framework.rpc.response_stream_type", "dedicated")
	viper.SetDefault("framework.rpc.max_retries", 3)
	viper.SetDefault("framework.rpc.timeout_ms", 5000)
	viper.SetDefault("framework.transport.broker_type", "redis")
	viper.SetDefault("framework.transport.redis_address", "redis://localhost:6379")
	viper.SetDefault("framework.transport.redis_cache", false)
	viper.SetDefault("framework.transport.use_trim_exact", false)
	viper.SetDefault("framework.transport.use_cluster", false)
	viper.SetDefault("framework.transport.pool_count", 10)
	viper.SetDefault("framework.transport.wait_time_ms", 15000)
	viper.SetDefault("framework.transport.use_encryption", true)
	viper.SetDefault("framework.transport.use_compression", true)
	viper.SetDefault("framework.transport.tls_required", false)
	viper.SetDefault("framework.logging.level", "info")
	viper.SetDefault("framework.logging.format", "json")
	viper.SetDefault("framework.logging.redact_sensitive", true)
	viper.SetDefault("framework.logging.debug_enabled", false)
	viper.SetDefault("framework.observability.tracing_enabled", true)
	viper.SetDefault("framework.observability.opentracing_host", "localhost")
	viper.SetDefault("framework.observability.opentracing_port", 6832)
	viper.SetDefault("framework.observability.metrics_adapter", "statsd")
	viper.SetDefault("framework.observability.metrics_host", "localhost")
	viper.SetDefault("framework.observability.metrics_port", 8125)
	viper.SetDefault("framework.observability.log_level_metrics", true)
	viper.SetDefault("framework.security.enable_jwt", true)
	viper.SetDefault("framework.security.jwt_signing_method", "HS256")
	viper.SetDefault("framework.security.enable_rbac", true)
	viper.SetDefault("framework.security.tls_strict", false)
	viper.SetDefault("framework.enable_dynamic_reload", false) // Dynamic reload disabled by default
}
