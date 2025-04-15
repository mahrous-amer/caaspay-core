package config

import (
	"os"
	"testing"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	// Reset Viper
	viper.Reset()

	// Load default configuration without any config file
	setDefaults()
	var config Config
	err := viper.Unmarshal(&config)

	assert.NoError(t, err, "Default configuration should load without errors")
	assert.Equal(t, "example-service", config.Framework.ServiceName, "Default service name should be 'example-service'")
	assert.Equal(t, "redis://localhost:6379", config.Framework.Transport.RedisAddress, "Default Redis address should be 'redis://localhost:6379'")
	assert.False(t, config.Framework.EnableDynamicReload, "Dynamic reload should be disabled by default")
}

func TestLoadConfigFromFile(t *testing.T) {
	// Create a temporary config file
	tempFile, err := os.CreateTemp("", "test_config_*.yaml")
	assert.NoError(t, err, "Temporary config file creation should succeed")
	defer os.Remove(tempFile.Name())

	content := `
framework:
  service_name: test-service
  transport:
    redis_address: redis://test-redis:6379
  enable_dynamic_reload: true
app_name: TestApp
encryption_key: TestKey
`
	_, err = tempFile.WriteString(content)
	assert.NoError(t, err, "Writing to temporary config file should succeed")

	// Load the config from the temporary file
	viper.Reset()
	config, err := LoadConfig(tempFile.Name(), "")

	assert.NoError(t, err, "Loading configuration from file should succeed")
	assert.Equal(t, "test-service", config.Framework.ServiceName, "Service name should match the config file")
	assert.Equal(t, "redis://test-redis:6379", config.Framework.Transport.RedisAddress, "Redis address should match the config file")
	assert.True(t, config.Framework.EnableDynamicReload, "Dynamic reload should be enabled as per the config file")
	assert.Equal(t, "TestApp", config.AppName, "App name should match the config file")
	assert.Equal(t, "TestKey", config.EncryptionKey, "Encryption key should match the config file")
}

func TestEnvironmentOverrides(t *testing.T) {
	// Set environment variables
	os.Setenv("APP_SERVICE_NAME", "env-service")
	os.Setenv("APP_REDIS_ADDRESS", "redis://env-redis:6379")
	defer os.Unsetenv("APP_SERVICE_NAME")
	defer os.Unsetenv("APP_REDIS_ADDRESS")

	// Reset Viper and load defaults
	viper.Reset()
	setDefaults()

	// Bind environment variables
	_ = viper.BindEnv("framework.service_name", "APP_SERVICE_NAME")
	_ = viper.BindEnv("framework.transport.redis_address", "APP_REDIS_ADDRESS")

	var config Config
	err := viper.Unmarshal(&config)

	assert.NoError(t, err, "Configuration should load without errors")
	assert.Equal(t, "env-service", config.Framework.ServiceName, "Service name should be overridden by environment variable")
	assert.Equal(t, "redis://env-redis:6379", config.Framework.Transport.RedisAddress, "Redis address should be overridden by environment variable")
}

func TestValidation(t *testing.T) {
	config := Config{
		Framework: FrameworkConfig{
			Transport: TransportConfig{
				RedisAddress: "",
			},
		},
		AppName:       "",
		EncryptionKey: "",
	}

	err := config.Validate()
	assert.Error(t, err, "Validation should fail if required fields are missing")
	assert.Contains(t, err.Error(), "RedisAddress", "Error should mention missing RedisAddress")
	assert.Contains(t, err.Error(), "AppName", "Error should mention missing AppName")
	assert.Contains(t, err.Error(), "EncryptionKey", "Error should mention missing EncryptionKey")

	// Update config to include all required fields
	config.Framework.Transport.RedisAddress = "redis://valid-redis:6379"
	config.AppName = "ValidApp"
	config.EncryptionKey = "ValidKey"

	// Validation should pass when all required fields are present
	err = config.Validate()
	assert.NoError(t, err, "Validation should pass if all required fields are set")
}

func TestDynamicReload(t *testing.T) {
	// Create a temporary config file
	tempFile, err := os.CreateTemp("", "test_config_reload_*.yaml")
	assert.NoError(t, err, "Temporary config file creation should succeed")
	defer os.Remove(tempFile.Name())

	content := `
framework:
  service_name: reload-service
  enable_dynamic_reload: true
app_name: ReloadApp
encryption_key: ReloadKey
`
	_, err = tempFile.WriteString(content)
	assert.NoError(t, err, "Writing to temporary config file should succeed")

	// Load the config from the temporary file
	viper.Reset()
	config, err := LoadConfig(tempFile.Name(), "")
	assert.NoError(t, err, "Loading configuration from file should succeed")
	assert.Equal(t, "reload-service", config.Framework.ServiceName, "Service name should match the config file")
	assert.True(t, config.Framework.EnableDynamicReload, "Dynamic reload should be enabled as per the config file")

	// Update the file to simulate a config change
	newContent := `
framework:
  service_name: updated-service
  enable_dynamic_reload: true
app_name: UpdatedApp
encryption_key: UpdatedKey
`
	err = os.WriteFile(tempFile.Name(), []byte(newContent), 0644)
	assert.NoError(t, err, "Updating temporary config file should succeed")

	// Wait for the reload to take effect (simulate with a small delay)
	viper.OnConfigChange(func(e fsnotify.Event) {
		err := viper.Unmarshal(&config)
		assert.NoError(t, err, "Reloading configuration after file change should succeed")
		assert.Equal(t, "updated-service", config.Framework.ServiceName, "Service name should reflect updated config file")
		assert.Equal(t, "UpdatedApp", config.AppName, "App name should reflect updated config file")
		assert.Equal(t, "UpdatedKey", config.EncryptionKey, "Encryption key should reflect updated config file")
	})
	viper.WatchConfig()
}
