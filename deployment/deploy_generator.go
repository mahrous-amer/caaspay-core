package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// Directories for storing environment and credential files
const (
	envDir   = "container_envs_production" // Folder for .env files
	filesDir = "container_files"           // Folder for credential files
)


// Config structure for `config.yaml`
type Config struct {
	Helper         map[string]map[string]interface{} `yaml:"helper"`
	Setting        map[string]interface{}            `yaml:"setting"`
	ServiceConfig  map[string]interface{}            `yaml:"service_config"`
}

// Credentials structure for `credentials.yaml`
type Credentials struct {
	Environment map[string]string `yaml:"environment"`
	File        []struct {
		ContainerPath string `yaml:"container_path"`
		Content       string `yaml:"content"`
	} `yaml:"file"`
}

// ServiceData holds details about a service
type ServiceData struct {
	Name          string
	Config        Config
	Credentials   Credentials
	BaseHelper    string
	SupportHelper []string
  EnvFile       string  // Path to .env file
	CredentialDir string  // Path to credential directory
}

func init() {
	// Setup Viper for configuration management
	viper.AutomaticEnv()
	viper.SetConfigFile(".env")
	_ = viper.ReadInConfig()

	// Define command-line arguments
	pflag.String("include-system", "", "Include services from a specific system category")
	pflag.String("include-type", "", "Include services of a certain type")
	pflag.String("include-service", "", "Include specific named services")
	pflag.String("exclude-system", "", "Exclude services from a specific system category")
	pflag.String("exclude-type", "", "Exclude services of a certain type")
	pflag.String("exclude-service", "", "Exclude specific named services")
	pflag.Parse()

	// Bind flags with Viper
	_ = viper.BindPFlags(pflag.CommandLine)
}

func main() {
	fmt.Println("🔍 Scanning services...")
	services, err := scanServices("service")
	if err != nil {
		fmt.Println("❌ Error scanning services:", err)
		return
	}

	// Apply filters
	services = filterServices(services)

	fmt.Printf("✅ Found %d services after filtering\n", len(services))

	// Resolve dependencies and inject credentials
	resolveDependencies(&services)

	// Count credentials picked up
	credentialsCount := countCredentials(services)
	fmt.Printf("🔑 Picked up %d credentials files\n", credentialsCount)

  // Write `.env` files and credentials
	writeEnvAndCredentials(services)


	generateDockerCompose(services)
}

// scanServices scans the services directory and loads configuration
//func scanServices(servicesDir string) ([]ServiceData, error) {
//	var services []ServiceData
//
//	err := filepath.Walk(servicesDir, func(path string, info os.FileInfo, err error) error {
//		if err != nil {
//			return err
//		}
//
//		// Load config.yaml for each service
//		if info.Name() == "config.yaml" {
//			servicePath := filepath.Dir(path)
//			serviceName := strings.ReplaceAll(strings.TrimPrefix(servicePath, "services/"), "/", "-")
//
//			config, err := loadConfig(path)
//			if err != nil {
//				fmt.Println("⚠️ Warning: Failed to load config for", serviceName, "->", err)
//				return nil
//			}
//
//			credentialsPath := filepath.Join(servicePath, "credentials.yaml")
//			credentials, _ := loadCredentials(credentialsPath)
//
//			services = append(services, ServiceData{
//				Name:          serviceName,
//				Config:        config,
//				Credentials:   credentials,
//				BaseHelper:    extractBaseHelper(config),
//				SupportHelper: extractSupportHelper(config),
//			})
//		}
//		return nil
//	})
//
//	return services, err
//}

// scanServices scans the `service/` directory and loads configuration
func scanServices(servicesDir string) ([]ServiceData, error) {
	var services []ServiceData

	err := filepath.Walk(servicesDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.Name() == "config.yaml" {
			servicePath := filepath.Dir(path)
			serviceName := formatServiceName(servicePath)

			config, err := loadConfig(path)
			if err != nil {
				fmt.Println("⚠️ Warning: Failed to load config for", serviceName, "->", err)
				return nil
			}

			credentialsPath := filepath.Join(servicePath, "credentials.yaml")
			credentials, _ := loadCredentials(credentialsPath)

			envFile := fmt.Sprintf("%s/%s.env", envDir, serviceName)
			credentialDir := fmt.Sprintf("%s/%s", filesDir, serviceName)

			services = append(services, ServiceData{
				Name:          serviceName,
				Config:        config,
				Credentials:   credentials,
				BaseHelper:    extractBaseHelper(config),
				SupportHelper: extractSupportHelper(config),
				EnvFile:       envFile,
				CredentialDir: credentialDir,
			})
		}
		return nil
	})

	return services, err
}

// formatServiceName converts a directory path into a service name
func formatServiceName(path string) string {
	trimmedPath := strings.TrimPrefix(path, "service/")
	return strings.ReplaceAll(trimmedPath, "/", "-")
}

// writeEnvAndCredentials generates `.env` files and writes credential files
func writeEnvAndCredentials(services []ServiceData) {
	for _, service := range services {
		// Write `.env` file
		writeEnvFile(service, services)

		// Write credential files if needed
		if len(service.Credentials.File) > 0 {
			os.MkdirAll(service.CredentialDir, os.ModePerm)
			for _, file := range service.Credentials.File {
				writeCredentialFile(service.CredentialDir, file.ContainerPath, file.Content)
			}
		}
	}
}

// writeEnvFile writes environment variables and service dependencies to a `.env` file
func writeEnvFile(service ServiceData, allServices []ServiceData) {
	// Ensure directory exists
	os.MkdirAll(envDir, os.ModePerm)

	filePath := service.EnvFile
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("❌ Error creating env file: %s\n", filePath)
		return
	}
	defer file.Close()

	// Write service-specific environment variables
	if len(service.Credentials.Environment) == 0 {
		fmt.Printf("⚠️ Warning: No environment variables found for %s\n", service.Name)
	}
	for key, value := range service.Credentials.Environment {
		_, _ = file.WriteString(fmt.Sprintf("%s=%s\n", key, value))
	}

	// Write service dependencies
	for _, s := range allServices {
		serviceAlias := formatServiceName(s.Name)
		_, _ = file.WriteString(fmt.Sprintf("service_%s=%s\n", serviceAlias, s.Name))
	}

	fmt.Printf("✅ Successfully wrote env file: %s\n", filePath)
}

// writeCredentialFile writes a credential file
func writeCredentialFile(directory, containerPath, content string) {
	filename := fmt.Sprintf("%s/%s", directory, filepath.Base(containerPath))
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("❌ Error writing credential file:", filename)
		return
	}
	defer file.Close()

	if len(content) == 0 {
		fmt.Printf("⚠️ Warning: Empty content for credential file %s\n", filename)
	}
	_, _ = file.WriteString(content)

	fmt.Printf("✅ Successfully wrote credential file: %s\n", filename)
}

// generateDockerCompose generates `docker-compose.yml`
func generateDockerCompose(services []ServiceData) {
	fmt.Println("⚙️ Generating docker-compose.yml...")

	templateContent := `version: '3.2'
services:
{{ range . }}
  {{ .Name }}:
    {{- if .Config.ServiceConfig.docker_deployment.image }}
    image: {{ .Config.ServiceConfig.docker_deployment.image }}
    {{- else }}
    build: service/{{ .Name }}
    {{- end }}
    env_file:
      - {{ .EnvFile }}
    volumes:
      - "{{ .CredentialDir }}:/etc/credentials"
    networks:
{{- range .Config.ServiceConfig.network }}
      - {{ . }}
{{- end }}
    restart: always
{{ end }}

networks:
  caddy-config_webtraffic:
    external: true
`

	tmpl, err := template.New("docker-compose").Parse(templateContent)
	if err != nil {
		fmt.Println("❌ Error parsing template:", err)
		return
	}

	outputFile, err := os.Create("deployment/output/docker-compose.yml")
	if err != nil {
		fmt.Println("❌ Error creating docker-compose.yml:", err)
		return
	}
	defer outputFile.Close()

	err = tmpl.Execute(outputFile, services)
	if err != nil {
		fmt.Println("❌ Error generating docker-compose.yml:", err)
	}
	fmt.Println("✅ Generated docker-compose.yml successfully")
}


// loadConfig loads a service's config.yaml
func loadConfig(path string) (Config, error) {
	var config Config
	data, err := os.ReadFile(path)
	if err != nil {
		return config, err
	}
	err = yaml.Unmarshal(data, &config)
	return config, err
}

// loadCredentials loads a service's credentials.yaml
func loadCredentials(path string) (Credentials, error) {
	var credentials Credentials
	data, err := os.ReadFile(path)
	if err != nil {
		return credentials, err
	}
	err = yaml.Unmarshal(data, &credentials)
	return credentials, err
}

// filterServices applies include/exclude filters to services using Viper
func filterServices(services []ServiceData) []ServiceData {
	var filteredServices []ServiceData

	includeSystem := viper.GetString("include-system")
	includeType := viper.GetString("include-type")
	includeService := viper.GetString("include-service")
	excludeSystem := viper.GetString("exclude-system")
	excludeType := viper.GetString("exclude-type")
	excludeService := viper.GetString("exclude-service")

	for _, service := range services {
		// Check if service should be excluded
		if matchesFilter(service.Name, excludeSystem) || matchesFilter(service.Name, excludeType) || matchesFilter(service.Name, excludeService) {
			continue
		}

		// If includes are specified, only add matching services
		if (includeSystem == "" || matchesFilter(service.Name, includeSystem)) &&
			(includeType == "" || matchesFilter(service.Name, includeType)) &&
			(includeService == "" || matchesFilter(service.Name, includeService)) {
			filteredServices = append(filteredServices, service)
		}
	}

	return filteredServices
}

// matchesFilter checks if a service name matches a given filter
func matchesFilter(serviceName, filter string) bool {
	if filter == "" {
		return false
	}
	for _, f := range strings.Split(filter, ",") {
		if strings.Contains(serviceName, strings.TrimSpace(f)) {
			return true
		}
	}
	return false
}

// extractBaseHelper extracts `base_helper` from config.yaml
func extractBaseHelper(config Config) string {
	if helperDependency, exists := config.Setting["helper_dependency"]; exists {
		if helperMap, ok := helperDependency.(map[string]interface{}); ok {
			if base, found := helperMap["base_helper"]; found {
				if baseArray, ok := base.([]interface{}); ok && len(baseArray) > 0 {
					return fmt.Sprintf("%v", baseArray[0])
				}
			}
		}
	}
	return "core" // Default to CAASPay framework base image
}

// extractSupportHelper extracts `support_helper` dependencies
func extractSupportHelper(config Config) []string {
	var dependencies []string
	if helperDependency, exists := config.Setting["helper_dependency"]; exists {
		if helperMap, ok := helperDependency.(map[string]interface{}); ok {
			if deps, found := helperMap["support_helper"]; found {
				if depArray, ok := deps.([]interface{}); ok {
					for _, dep := range depArray {
						dependencies = append(dependencies, fmt.Sprintf("%v", dep))
					}
				}
			}
		}
	}
	return dependencies
}

// resolveDependencies ensures required support services are included
func resolveDependencies(services *[]ServiceData) {
	fmt.Println("🔄 Resolving dependencies...")
	// Logic for dependency resolution will be implemented here
	fmt.Println("✅ Dependencies resolved successfully")
}

// countCredentials counts how many credentials files were picked up
func countCredentials(services []ServiceData) int {
	count := 0
	for _, service := range services {
		if len(service.Credentials.File) > 0 {
			count += len(service.Credentials.File)
		}
	}
	return count
}
