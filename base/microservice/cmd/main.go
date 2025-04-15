package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caaspay/caaspay-core/internal/framework"
	"github.com/caaspay/caaspay-core/internal/service"
)

func main() {
	// Step 1: Initialize framework context
	fwContext, err := framework.NewFrameworkContext()
	if err != nil {
		log.Fatalf("❌ Failed to initialize framework context: %v", err)
	}

	// Step 2: Initialize service
	serviceStruct, err := initializeService(fwContext)
	if err != nil {
		fwContext.Logger.Error(context.Background(), "❌ Service initialization failed", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Step 3: Run the service with graceful shutdown
	runService(serviceStruct, fwContext)
}

// initializeService sets up the service with the provided framework context.
func initializeService(fwContext *framework.FrameworkContext) (*service.ServiceStruct, error) {
	start := time.Now()
	svc := service.NewService()
	serviceStruct, err := service.NewServiceStruct(fwContext, svc)
	if err != nil {
		return nil, err
	}
	fwContext.Logger.Info(context.Background(), "✅ Service initialized", map[string]interface{}{
		"duration": time.Since(start).String(),
	})
	return serviceStruct, nil
}

// runService starts the service and handles graceful shutdown.
func runService(serviceStruct *service.ServiceStruct, fwContext *framework.FrameworkContext) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to capture OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the service in a separate goroutine
	go func() {
		fwContext.Logger.Info(ctx, "🚀 Service is starting...", nil)
		serviceStruct.Run(serviceStruct)
	}()

	// Wait for a shutdown signal
	sig := <-sigChan
	fwContext.Logger.Info(ctx, "⚠️ Shutdown signal received", map[string]interface{}{
		"signal": sig.String(),
	})

	// Create a shutdown context with a timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 30*time.Second)
	defer shutdownCancel()

	// Gracefully shut down the service
	fwContext.Logger.Info(ctx, "🛑 Shutting down service...", nil)
	serviceStruct.Shutdown()

	// Wait for all goroutines to complete or timeout
	select {
	case <-shutdownCtx.Done():
		if shutdownCtx.Err() == context.DeadlineExceeded {
			fwContext.Logger.Error(ctx, "❌ Shutdown timed out. Forcing exit.", nil)
		}
		os.Exit(1)
	}

	fwContext.Logger.Info(ctx, "✅ Service stopped gracefully", nil)
}
