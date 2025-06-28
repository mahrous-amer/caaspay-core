package api

import (
	"fmt"
	"github.com/caaspay/caaspay-core/pkg/common/metrics"
	"github.com/caaspay/caaspay-core/pkg/common/storage"
	"time"
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
	ServiceName         string                      `mapstructure:"service_name"`
	InstanceID          string                      `mapstructure:"instance_id"`
	Version             string                      `mapstructure:"version"`
	RPC                 RPCConfig                   `mapstructure:"rpc"`
	Transport           TransportConfig             `mapstructure:"transport"`
	HTTPClient          HTTPClientConfig            `mapstructure:"httpclient"`
	Logging             LoggingConfig               `mapstructure:"logging"`
	HealthCheck         HealthConfig                `mapstructure:"health_check"`
	Observability       metrics.ObservabilityConfig `mapstructure:"observability"`
	Security            SecurityConfig              `mapstructure:"security"`
	Storage             storage.StorageConfig       `mapstructure:"storage"`
	EnableDynamicReload bool                        `mapstructure:"enable_dynamic_reload"`
}

// RPCConfig contains settings for handling RPC responses.
type RPCConfig struct {
	ResponseStreamType string `mapstructure:"response_stream_type"` // "single" or "dedicated"
	MaxRetries         int    `mapstructure:"max_retries"`          // Retry failed RPC calls
	TimeoutMs          int    `mapstructure:"timeout_ms"`           // Default timeout for RPC calls
}

type HTTPClientConfig struct {
	Timeout   time.Duration `mapstructure:"timeout"`
	UserAgent string        `mapstructure:"user_agent"`
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

// SecurityConfig contains security-related settings.
type SecurityConfig struct {
	EnableJWT        bool   `mapstructure:"enable_jwt"`
	JWTSigningMethod string `mapstructure:"jwt_signing_method"` // "HS256", "RS256", etc.
	EnableRBAC       bool   `mapstructure:"enable_rbac"`
	TLSStrict        bool   `mapstructure:"tls_strict"` // Enforce strict TLS connections
}

// ServiceConfig defines service-specific configurations.
type ServiceConfig struct {
	Port        int    `json:"port" mapstructure:"port"`               // Port for the service
	Environment string `json:"environment" mapstructure:"environment"` // Environment: development, staging, production
	DebugMode   bool   `json:"debug_mode" mapstructure:"debug_mode"`   // Enable or disable debug mode
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
	ResponseStreamSubOnce   bool          `mapstructure:"response_stream_sub_once"`   // Subscribe to RPC response stream once throuout or on every request
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
