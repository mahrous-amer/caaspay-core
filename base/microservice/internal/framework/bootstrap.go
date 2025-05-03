package framework

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caaspay/caaspay-core/pkg/api"
)

// Bootstrap simplifies service startup and lifecycle management.
func Bootstrap(create func(*FrameworkContext) api.ServiceInterface) {
	ctx := context.Background()

	// Step 1: Init framework
	fwCtx, err := NewFrameworkContext()
	if err != nil {
		log.Fatalf("❌ Failed to initialize framework: %v", err)
	}

	// Step 2: Create developer service and inject
	svc := create(fwCtx)
	fwCtx.ServiceInstance = svc

	// Step 3: Build ServiceStruct to wire framework + logic
	svcStruct, err := service.NewServiceStruct(fwCtx, svc)
	if err != nil {
		fwCtx.Logger.Error(ctx, "❌ Service initialization failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}

	fwCtx.Logger.Info(ctx, "✅ Service initialized", nil)

	// Step 4: Start service
	go func() {
		fwCtx.Logger.Info(ctx, "🚀 Service is starting...", nil)
		svcStruct.Run()
	}()

	// Step 5: Listen for shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	fwCtx.Logger.Info(ctx, "⚠️ Shutdown signal received", map[string]interface{}{"signal": sig.String()})
	fwCtx.Logger.Info(ctx, "🛑 Initiating graceful shutdown...", nil)
	svcStruct.Shutdown()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	select {
	case <-svcStruct.Done():
		fwCtx.Logger.Info(ctx, "✅ Service stopped gracefully", nil)
	case <-shutdownCtx.Done():
		fwCtx.Logger.Error(ctx, "❌ Shutdown timed out. Forcing exit.", nil)
		os.Exit(1)
	}
}

