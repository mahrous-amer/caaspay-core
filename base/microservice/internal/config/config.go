package config

import (
	"fmt"
	"os"
	"encoding/json" // Importing the JSON package for marshaling/unmarshaling
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// Config represents the combined framework and service configurations.
type Config struct {
	Framework         FrameworkConfig       `mapstructure:"framework"`
	Service           map[string]interface{} `mapstructure:"service"` // Holds service-specific configs
	ComplianceEnabled bool                  `mapstructure:"compliance_enabled"`
	AppName           string                `mapstructure:"app_name"`
	Env               string                `mapstructure:"env"`
	PCIEnabled        bool                  `mapstructure:"pci_enabled"`
	EncryptionKey     string                `mapstructure:"encryption_key"`
}

// FrameworkConfig contains settings for the core framework.
type FrameworkConfig struct {
	ServiceName       string              `mapstructure:"service_name"`
	Version           string              `mapstructure:"version"`
	RPC               RPCConfig           `mapstructure:"rpc"`
	Transport         TransportConfig     `mapstructure:"transport"`
	Logging           LoggingConfig       `mapstructure:"logging"`
	Observability     ObservabilityConfig `mapstructure:"observability"`
	Security          SecurityConfig      `mapstructure:"security"`
	Storage           StorageConfig       `mapstructure:"storage"`
	EnableDynamicReload bool              `mapstructure:"enable_dynamic_reload"`
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

// StorageConfig defines storage-related configurations.
type StorageConfig struct {
	Type string `yaml:"type"` // Example: "inmemory", "redis", "sql"
}

// ServiceConfig defines service-specific configurations.
type ServiceConfig struct {
	Port        int    `json:"port" mapstructure:"port"`             // Port for the service
	Environment string `json:"environment" mapstructure:"environment"` // Environment: development, staging, production
	DebugMode   bool   `json:"debug_mode" mapstructure:"debug_mode"`  // Enable or disable debug mode
}

// DefaultServiceConfig returns default values for service-specific configurations.
func DefaultServiceConfig() *ServiceConfig {
	return &ServiceConfig{
		Port:        8080,            // Default port
		Environment: "development",  // Default environment
		DebugMode:   true,           // Debug mode enabled by default
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
    fmt.Printf("⚠️ No environment-specific config files found, using defaults + env vars from %s\n", absPath)
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
	v.SetDefault("framework.version", "1.0.0")
	v.SetDefault("framework.rpc.response_stream_type", "dedicated")
	v.SetDefault("framework.rpc.max_retries", 3)
	v.SetDefault("framework.rpc.timeout_ms", 5000)
	v.SetDefault("framework.transport.broker_type", "redis")
	v.SetDefault("framework.transport.redis_address", "redis://localhost:6379")
	v.SetDefault("framework.transport.redis_cache", false)
	v.SetDefault("framework.transport.use_trim_exact", false)
	v.SetDefault("framework.transport.use_cluster", false)
	v.SetDefault("framework.transport.pool_count", 10)
	v.SetDefault("framework.transport.wait_time_ms", 15000)
	v.SetDefault("framework.transport.use_encryption", true)
	v.SetDefault("framework.transport.use_compression", true)
	v.SetDefault("framework.transport.tls_required", false)
	v.SetDefault("framework.logging.level", "info")
	v.SetDefault("framework.logging.format", "json")
	v.SetDefault("framework.logging.redact_sensitive", true)
	v.SetDefault("framework.logging.debug_enabled", false)
	v.SetDefault("framework.observability.tracing_enabled", true)
	v.SetDefault("framework.observability.opentracing_host", "localhost")
	v.SetDefault("framework.observability.opentracing_port", 6832)
	v.SetDefault("framework.observability.metrics_adapter", "statsd")
	v.SetDefault("framework.observability.metrics_host", "localhost")
	v.SetDefault("framework.observability.metrics_port", 8125)
	v.SetDefault("framework.observability.log_level_metrics", true)
	v.SetDefault("framework.security.enable_jwt", true)
	v.SetDefault("framework.security.jwt_signing_method", "HS256")
	v.SetDefault("framework.security.enable_rbac", true)
	v.SetDefault("framework.security.tls_strict", false)
	v.SetDefault("framework.enable_dynamic_reload", false) // Dynamic reload disabled by default
  v.SetDefault("framework.storage.type", "inmemory")
}
