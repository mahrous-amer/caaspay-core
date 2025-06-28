package config

import (
	"encoding/json" // Importing the JSON package for marshaling/unmarshaling
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/caaspay/caaspay-core/pkg/api"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// DefaultServiceConfig returns default values for service-specific configurations.
func DefaultServiceConfig() *api.ServiceConfig {
	return &api.ServiceConfig{
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
func LoadConfig() (*api.Config, error) {
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

	var config api.Config
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
	v.SetDefault("framework.transport.response_stream_sub_once", true)
	v.SetDefault("framework.transport.pool_size", 10)
	v.SetDefault("framework.transport.min_idle_conns", 2)
	v.SetDefault("framework.transport.stream_read_count", 10)
	v.SetDefault("framework.transport.dial_timeout", 5*time.Second)
	v.SetDefault("framework.transport.read_timeout", 20*time.Second)
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
	v.SetDefault("framework.httpclient.timeout", 60*time.Second)
	v.SetDefault("framework.httpclient.user_agent", "CAASPay/1.0 (service; +https://caaspay.com; contact=requestsinfo@caaspay.com)")
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
