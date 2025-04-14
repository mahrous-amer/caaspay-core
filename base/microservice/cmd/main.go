package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caaspay/caaspay-core/internal/config"
	"github.com/caaspay/caaspay-core/internal/logging"
	"github.com/caaspay/caaspay-core/internal/service"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("config/config.yaml", "config/service.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize logger
	logger := logging.NewLogger(cfg.Framework.ServiceName, cfg.Framework.Logging.Level, cfg.Framework.Logging.RedactSensitive)

	// Create service instance
	svc := service.NewService()

	// Create service struct
	serviceStruct, err := service.NewServiceStruct(cfg, svc)
	if err != nil {
		logger.Error(context.Background(), "Failed to create service struct", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the service
	go func() {
		serviceStruct.Run(svc)
	}()

	// Wait for shutdown signal
	<-sigChan
	logger.Info(context.Background(), "Shutdown signal received", nil)

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown the service
	serviceStruct.Shutdown()

	// Wait for all goroutines to complete
	select {
	case <-ctx.Done():
		logger.Error(context.Background(), "Shutdown timeout", nil)
		os.Exit(1)
	}
}