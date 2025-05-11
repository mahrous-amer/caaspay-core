package config

import (
	"encoding/json" // Importing the JSON package for marshaling/unmarshaling
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// Config represents the combined framework and service configurations.
type Config struct {
	Framework         FrameworkConfig        `mapstructure:"framework"`
	Service           map[string]interface{} `mapstructure:"service"` // Holds service-specific configs
	ComplianceEnabled bool                   `mapstructure:"compliance_enabled"`
	AppName           string                 `mapstructure:"app_name"`
	Env               string                 `mapstructure:"env"`
	PCIEnabled        bool                   `mapstructure:"pci_enabled"`
	EncryptionKey     string                 `mapstructure:"encryption_key"`
}

// FrameworkConfig contains settings for the core framework.
type FrameworkConfig struct {
	ServiceName         string              `mapstructure:"service_name"`
	InstanceID          string              `mapstructure:"instance_id"`
	Version             string              `mapstructure:"version"`
	RPC                 RPCConfig           `mapstructure:"rpc"`
	Transport           TransportConfig     `mapstructure:"transport"`
	Logging             LoggingConfig       `mapstructure:"logging"`
	HealthCheck         HealthConfig        `mapstructure:"health_check"`
	Observability       ObservabilityConfig `mapstructure:"observability"`
	Security            SecurityConfig      `mapstructure:"security"`
	Storage             StorageConfig       `mapstructure:"storage"`
	EnableDynamicReload bool                `mapstructure:"enable_dynamic_reload"`
}

// RPCConfig contains settings for handling RPC responses.
type RPCConfig struct {
	ResponseStreamType string `mapstructure:"response_stream_type"` // "single" or "dedicated"
	MaxRetries         int    `mapstructure:"max_retries"`          // Retry failed RPC calls
	TimeoutMs          int    `mapstructure:"timeout_ms"`           // Default timeout for RPC calls
}

// TransportConfig contains messaging transport settings.
type TransportConfig struct {
	BrokerType  string   `mapstructure:"broker_type"`  // "redis", "nats", "kafka" (future extensibility)
	RedisAddr   []string `mapstructure:"redis_addr"`   // Redis server address or cluster endpoint
	Password    string   `mapstructure:"password"`     // Optional Redis AUTH password
	DB          int      `mapstructure:"db"`           // Optional DB selection
	UseCluster  bool     `mapstructure:"use_cluster"`  // Use Redis Cluster mode
	TLSRequired bool     `mapstructure:"tls_required"` // Enable TLS for Redis connections

	UseCompression bool   `mapstructure:"use_compression"` // Enable message compression
	UseEncryption  bool   `mapstructure:"use_encryption"`  // Enable message encryption
	EncryptionKey  string `mapstructure:"encryption_key"`  // Encryption key for AES

	ServiceReplyStream      string        `mapstructure:"service_reply_stream"`       // Default reply stream for RPC
	ResponseOnServiceStream bool          `mapstructure:"response_on_service_stream"` // Use service stream for responses
	DLQStream               string        `mapstructure:"dlq_stream"`                 // Dead-letter stream for failed messages
	MoveExpiredToDLQ        bool          `mapstructure:"move_expired_to_dlq"`        // move expired messages to DLQ
	MaxRetries              int           `mapstructure:"max_retries"`                // Retry attempts for transient failures
	RetryDelay              time.Duration `mapstructure:"retry_delay"`                // Delay between retry attempts
	StreamReadCount         int64         `mapstructure:"stream_read_count"`          // Number of messages to read from stream

	// Connection Pool and Timeout Options
	PoolSize         int           `mapstructure:"pool_size"`           // Max total connections
	MinIdleConns     int           `mapstructure:"min_idle_conns"`      // Minimum idle connections
	DialTimeout      time.Duration `mapstructure:"dial_timeout"`        // Timeout for initial connection
	ReadTimeout      time.Duration `mapstructure:"read_timeout"`        // Timeout for read operations
	WriteTimeout     time.Duration `mapstructure:"write_timeout"`       // Timeout for write operations
	PoolTimeout      time.Duration `mapstructure:"pool_timeout"`        // Max time to wait for a free connection
	ConnMaxIdleTime  time.Duration `mapstructure:"conn_max_idle_time"`  // Max idle time for connections
	ConnMaxLifetime  time.Duration `mapstructure:"conn_max_lifetime"`   // Max lifetime for a connection
	StreamTrimMaxLen int64         `mapstructure:"stream_trim_max_len"` // hard cap on stream length (e.g., 10000 entries)
	StreamTrimApprox bool          `mapstructure:"stream_trim_approx"`  // use ~ approximation (faster trim)
	PeriodicTrimFreq time.Duration `mapstructure:"periodic_trim_freq"`  // Frequency of periodic trimming
}

// LoggingConfig contains logging-related settings.
type LoggingConfig struct {
	Level           string `mapstructure:"level"`
	Format          string `mapstructure:"format"` // "json" or "text"
	RedactSensitive bool   `mapstructure:"redact_sensitive"`
	DebugEnabled    bool   `mapstructure:"debug_enabled"` // Toggle verbose debugging logs
}

// HealthConfig contains health and monitor related settings.
type HealthConfig struct {
	InternalHealthChecker bool          `mapstructure:"internal_health_checker"`  // If true, start internal goroutine health checker
	HTTPServerEnabled     bool          `mapstructure:"http_server_enabled"`      // If true, run HTTP server for health endpoints
	HTTPServerPort        int           `mapstructure:"http_server_port"`         // Which port to serve on (e.g., 8080)
	HTTPServerHealthRoute string        `mapstructure:"http_server_health_route"` // e.g., /healthz
	HTTPServerReadyRoute  string        `mapstructure:"http_server_ready_route"`  // e.g., /readyz
	HTTPServerLiveRoute   string        `mapstructure:"http_server_live_route"`   // e.g., /livez
	ExposeMetricsEndpoint bool          `mapstructure:"expose_metrics_endpoint"`
	MetricsRoute          string        `mapstructure:"metrics_route"`
	HeartbeatEnabled      bool          `mapstructure:"heartbeat_enabled"`
	HeartbeatInterval     time.Duration `mapstructure:"heartbeat_interval"`
}

// ObservabilityConfig contains observability settings.
type ObservabilityConfig struct {
	TracingEnabled  bool   `mapstructure:"tracing_enabled"` // OpenTelemetry support
	OpentracingHost string `mapstructure:"opentracing_host"`
	OpentracingPort int    `mapstructure:"opentracing_port"`
	MetricsAdapter  string `mapstructure:"metrics_adapter"`
	MetricsHost     string `mapstructure:"metrics_host"`
	MetricsPort     int    `mapstructure:"metrics_port"`
	LogLevelMetrics bool   `mapstructure:"log_level_metrics"` // Toggle log-based metrics
	Env             string `yaml:"env"`
}

// SecurityConfig contains security-related settings.
type SecurityConfig struct {
	EnableJWT        bool   `mapstructure:"enable_jwt"`
	JWTSigningMethod string `mapstructure:"jwt_signing_method"` // "HS256", "RS256", etc.
	EnableRBAC       bool   `mapstructure:"enable_rbac"`
	TLSStrict        bool   `mapstructure:"tls_strict"` // Enforce strict TLS connections
}

// StorageConfig defines storage-related configurations.
type StorageConfig struct {
	Type string `yaml:"type"` // Example: "inmemory", "redis", "sql"
}

// ServiceConfig defines service-specific configurations.
type ServiceConfig struct {
	Port        int    `json:"port" mapstructure:"port"`               // Port for the service
	Environment string `json:"environment" mapstructure:"environment"` // Environment: development, staging, production
	DebugMode   bool   `json:"debug_mode" mapstructure:"debug_mode"`   // Enable or disable debug mode
}

// DefaultServiceConfig returns default values for service-specific configurations.
func DefaultServiceConfig() *ServiceConfig {
	return &ServiceConfig{
		Port:        8080,          // Default port
		Environment: "development", // Default environment
		DebugMode:   true,          // Debug mode enabled by default
	}
}

// MapToStruct maps a map[string]interface{} to a struct.
func MapToStruct(data interface{}, out interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal map to JSON: %w", err)
	}

	if err := json.Unmarshal(jsonData, out); err != nil {
		return fmt.Errorf("failed to unmarshal JSON to struct: %w", err)
	}

	return nil
}

// LoadConfig dynamically loads configuration files and environment variables.
func LoadConfig() (*Config, error) {
	configDir := os.Getenv("CONFIG_DIRECTORY")
	if configDir == "" {
		configDir = "./config"
	}

	environment := os.Getenv("ENVIRONMENT")
	if environment == "" {
		environment = "development"
	}

	mainViper := viper.New()
	mainViper.SetEnvPrefix("APP")
	mainViper.AutomaticEnv()
	setDefaults(mainViper)

	files, err := os.ReadDir(configDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read config directory: %w", err)
	}

	merged := false

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".yaml") || !strings.HasPrefix(file.Name(), environment) {
			continue
		}

		filePath := filepath.Join(configDir, file.Name())
		fmt.Printf("📖 Reading config file: %s\n", filePath)

		envViper := viper.New()
		envViper.SetConfigFile(filePath)
		envViper.SetConfigType("yaml")

		if err := envViper.ReadInConfig(); err != nil {
			fmt.Printf("⚠️ Warning: Failed to read config %s: %v\n", filePath, err)
			continue
		}

		if err := mainViper.MergeConfigMap(envViper.AllSettings()); err != nil {
			fmt.Printf("⚠️ Warning: Failed to merge config %s: %v\n", filePath, err)
			continue
		}

		merged = true
	}

	if !merged {
		absPath, err := filepath.Abs(configDir)
		if err != nil {
			absPath = configDir
		}
		fmt.Printf("⚠️ No environment-specific (%s) config files found, using defaults + env vars from %s\n", environment, absPath)
	}

	var config Config
	if err := mainViper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	if config.Framework.EnableDynamicReload {
		mainViper.WatchConfig()
		mainViper.OnConfigChange(func(e fsnotify.Event) {
			fmt.Println("⚡ Configuration file changed:", e.Name)
			if err := mainViper.Unmarshal(&config); err != nil {
				fmt.Println("❌ Failed to reload config:", err)
			} else {
				fmt.Println("✅ Configuration reloaded successfully.")
			}
		})
	} else {
		fmt.Println("🛑 Dynamic config reloading disabled")
	}

	return &config, nil
}

// Validate ensures the configuration is complete and correct.
func (c *Config) Validate() error {
	var missingFields []string

	// Check for required fields and add them to the list of missing fields
	//if c.Framework.Transport.RedisAddress == "" {
	//	missingFields = append(missingFields, "RedisAddress")
	//}
	if c.Framework.ServiceName == "" {
		missingFields = append(missingFields, "Framework.ServiceName")
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
func setDefaults(v *viper.Viper) {
	v.SetDefault("app_name", "example")
	v.SetDefault("encryption_key", "Defaultfff")
	v.SetDefault("framework.service_name", "example-service")
	v.SetDefault("framework.instance_id", "main")
	v.SetDefault("framework.version", "1.0.0")
	v.SetDefault("framework.rpc.response_stream_type", "dedicated")
	v.SetDefault("framework.rpc.max_retries", 3)
	v.SetDefault("framework.rpc.timeout_ms", 5000)
	v.SetDefault("framework.transport.broker_type", "redis")
	v.SetDefault("framework.transport.redis_addr", []string{"redis_7:6379"})
	v.SetDefault("framework.transport.use_cluster", false)
	v.SetDefault("framework.transport.response_on_service_stream", false)
	v.SetDefault("framework.transport.pool_size", 10)
	v.SetDefault("framework.transport.min_idle_conns", 2)
	v.SetDefault("framework.transport.stream_read_count", 10)
	v.SetDefault("framework.transport.dial_timeout", 5*time.Second)
	v.SetDefault("framework.transport.read_timeout", 10*time.Second)
	v.SetDefault("framework.transport.retry_delay", 1*time.Second)
	v.SetDefault("framework.transport.write_timeout", 2*time.Second)
	v.SetDefault("framework.transport.pool_timeout", 1*time.Second)
	v.SetDefault("framework.transport.conn_max_idle_time", 20*time.Second)
	v.SetDefault("framework.transport.conn_max_lifetime", 200*time.Second)
	v.SetDefault("framework.transport.periodic_trim_freq", 5*time.Second)
	v.SetDefault("framework.transport.max_retries", 1000)
	v.SetDefault("framework.transport.use_encryption", true)
	v.SetDefault("framework.transport.use_compression", true)
	v.SetDefault("framework.transport.encryption_key", "1313c15c22701f9fd383b7f7d69efe7b86783605998990a9fb04b84f817defab")
	v.SetDefault("framework.transport.tls_required", false)
	v.SetDefault("framework.transport.move_expired_to_dlq", true)
	v.SetDefault("framework.transport.stream_trim_max_len", 10000)
	v.SetDefault("framework.transport.stream_trim_approx", true)
	v.SetDefault("framework.logging.level", "info")
	v.SetDefault("framework.logging.format", "json")
	v.SetDefault("framework.logging.redact_sensitive", true)
	v.SetDefault("framework.health_check.internal_health_checker", true)
	v.SetDefault("framework.health_check.http_server_enabled", true)
	v.SetDefault("framework.health_check.http_server_port", 8080)
	v.SetDefault("framework.health_check.http_server_health_route", "/healthz")
	v.SetDefault("framework.health_check.http_server_ready_route", "/readyz")
	v.SetDefault("framework.health_check.http_server_live_route", "/livez")
	v.SetDefault("framework.health_check.expose_metrics_endpoint", true)
	v.SetDefault("framework.health_check.metrics_route", "/metrics")
	v.SetDefault("framework.health_check.heartbeat_enabled", true)
	v.SetDefault("framework.health_check.heartbeat_interval", 10*time.Second)
	v.SetDefault("framework.logging.debug_enabled", false)
	v.SetDefault("framework.observability.tracing_enabled", true)
	v.SetDefault("framework.observability.opentracing_host", "localhost")
	v.SetDefault("framework.observability.opentracing_port", 6832)
	v.SetDefault("framework.observability.metrics_adapter", "statsd")
	v.SetDefault("framework.observability.metrics_host", "localhost")
	v.SetDefault("framework.observability.metrics_port", 8125)
	v.SetDefault("framework.observability.log_level_metrics", true)
	v.SetDefault("framework.observability.env", "test")
	v.SetDefault("framework.security.enable_jwt", true)
	v.SetDefault("framework.security.jwt_signing_method", "HS256")
	v.SetDefault("framework.security.enable_rbac", true)
	v.SetDefault("framework.security.tls_strict", false)
	v.SetDefault("framework.enable_dynamic_reload", false) // Dynamic reload disabled by default
	v.SetDefault("framework.storage.type", "inmemory")
}
