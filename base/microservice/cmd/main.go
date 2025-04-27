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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		fwContext.Logger.Info(ctx, "🚀 Service is starting...", nil)
		serviceStruct.Run()
	}()

	// Wait for a shutdown signal
	sig := <-sigChan
	fwContext.Logger.Info(ctx, "⚠️ Shutdown signal received", map[string]interface{}{
		"signal": sig.String(),
	})

	fwContext.Logger.Info(ctx, "🛑 Initiating graceful shutdown...", nil)
	serviceStruct.Shutdown()

	// Set timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	select {
	case <-serviceStruct.Done(): // 🆕 Wait until Run() finishes
		fwContext.Logger.Info(ctx, "✅ Service stopped gracefully", nil)
	case <-shutdownCtx.Done():
		fwContext.Logger.Error(ctx, "❌ Shutdown timed out. Forcing exit.", nil)
		os.Exit(1)
	}

	os.Exit(0)
}
