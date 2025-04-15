package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func setupTestEnvironment(t *testing.T, files map[string]string) (string, func()) {
	// Create a temporary directory for the test environment
	tempDir, err := os.MkdirTemp("", "config_test_*")
	assert.NoError(t, err, "Failed to create temporary config directory")

	// Write provided configuration files
	for filename, content := range files {
		filePath := filepath.Join(tempDir, filename)
		err := os.WriteFile(filePath, []byte(content), 0644)
		assert.NoError(t, err, "Failed to write test config file: %s", filename)
	}

	// Return the directory path and a cleanup function
	return tempDir, func() {
		os.RemoveAll(tempDir) // Clean up the temporary directory
	}
}

func TestLoadConfig_Success(t *testing.T) {
	// Setup test environment with dynamic files
	files := map[string]string{
		"testing-config1.yaml": `
framework:
  service_name: "testing-service"
  version: "1.0.0"
  enable_dynamic_reload: true
app_name: "test_app"
pci_enabled: true
`,
		"testing-config2.yaml": `
framework:
  enable_dynamic_reload: false
encryption_key: "test_encryption_key"
`,
	}
	tempDir, cleanup := setupTestEnvironment(t, files)
	defer cleanup()

	// Set environment variables
	os.Setenv("CONFIG_DIRECTORY", tempDir)
	os.Setenv("ENVIRONMENT", "testing")

	// Call LoadConfig
	config, err := LoadConfig()

	// Assertions
	assert.NoError(t, err, "LoadConfig should not return an error")
	assert.NotNil(t, config, "Config should not be nil")
	assert.Equal(t, "testing-service", config.Framework.ServiceName, "Framework.ServiceName mismatch")
	assert.Equal(t, "1.0.0", config.Framework.Version, "Framework.Version mismatch")
	assert.Equal(t, false, config.Framework.EnableDynamicReload, "Framework.EnableDynamicReload mismatch")
	assert.Equal(t, "test_app", config.AppName, "AppName mismatch")
	assert.Equal(t, "test_encryption_key", config.EncryptionKey, "EncryptionKey mismatch")
	assert.Equal(t, true, config.PCIEnabled, "PCIEnabled mismatch")
}

func TestLoadConfig_MissingDirectory(t *testing.T) {
	// Set environment variables to a non-existent directory
	os.Setenv("CONFIG_DIRECTORY", "./invalid_directory")
	os.Setenv("ENVIRONMENT", "testing")

	// Call LoadConfig
	_, err := LoadConfig()

	// Assertions
	assert.Error(t, err, "LoadConfig should return an error for a missing directory")

	// Cleanup
	os.Unsetenv("CONFIG_DIRECTORY")
	os.Unsetenv("ENVIRONMENT")
}

func TestValidation(t *testing.T) {
	config := Config{
		Framework: FrameworkConfig{
			ServiceName: "",
		},
		AppName:       "",
		EncryptionKey: "",
	}

	err := config.Validate()
	assert.Error(t, err, "Validation should fail if required fields are missing")
	assert.Contains(t, err.Error(), "Framework.ServiceName", "Error should mention missing Framework.ServiceName")
	assert.Contains(t, err.Error(), "AppName", "Error should mention missing AppName")
	assert.Contains(t, err.Error(), "EncryptionKey", "Error should mention missing EncryptionKey")

	// Update config to include all required fields
	config.Framework.ServiceName = "valid-service"
	config.AppName = "ValidApp"
	config.EncryptionKey = "ValidKey"

	// Validation should pass when all required fields are present
	err = config.Validate()
	assert.NoError(t, err, "Validation should pass if all required fields are set")
}

func TestDynamicReload(t *testing.T) {
	// Setup test environment with an initial config file
	files := map[string]string{
		"testing-config.yaml": `
framework:
  service_name: "reload-service"
  enable_dynamic_reload: true
app_name: "ReloadApp"
encryption_key: "ReloadKey"
`,
	}
	tempDir, cleanup := setupTestEnvironment(t, files)
	defer cleanup()

	// Set environment variables
	os.Setenv("CONFIG_DIRECTORY", tempDir)
	os.Setenv("ENVIRONMENT", "testing")

	// Load the initial configuration
	config, err := LoadConfig()
	assert.NoError(t, err, "LoadConfig should not return an error")
	assert.Equal(t, "reload-service", config.Framework.ServiceName, "Framework.ServiceName mismatch")
	assert.Equal(t, "ReloadApp", config.AppName, "AppName mismatch")
	assert.Equal(t, "ReloadKey", config.EncryptionKey, "EncryptionKey mismatch")

	// Update the file to simulate a config change
	newContent := `
framework:
  service_name: "updated-service"
  enable_dynamic_reload: true
app_name: "UpdatedApp"
encryption_key: "UpdatedKey"
`
	err = os.WriteFile(filepath.Join(tempDir, "testing-config.yaml"), []byte(newContent), 0644)
	assert.NoError(t, err, "Updating the config file should succeed")

	// Wait for the reload to take effect
	viper.OnConfigChange(func(e fsnotify.Event) {
		err := viper.Unmarshal(&config)
		assert.NoError(t, err, "Reloading configuration after file change should succeed")
		assert.Equal(t, "updated-service", config.Framework.ServiceName, "Framework.ServiceName mismatch after reload")
		assert.Equal(t, "UpdatedApp", config.AppName, "AppName mismatch after reload")
		assert.Equal(t, "UpdatedKey", config.EncryptionKey, "EncryptionKey mismatch after reload")
	})
	viper.WatchConfig()

	// Allow some time for the reload handler to execute
	time.Sleep(1 * time.Second)
}
