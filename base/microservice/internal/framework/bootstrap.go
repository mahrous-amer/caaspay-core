package framework

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/caaspay/caaspay-core/internal/service"
	"github.com/caaspay/caaspay-core/pkg/api"
)

// Bootstrap simplifies service startup and lifecycle management.
func Bootstrap(create func(api.FrameworkContextInterface) api.ServiceInterface) {
	// Step 1: Create unified root context
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Step 2: Initialize the framework
	fwCtx, err := NewFrameworkContext(rootCtx)
	if err != nil {
		log.Fatalf("❌ Failed to initialize framework: %v", err)
	}

	// Step 3: Create developer service instance and register
	svc := create(fwCtx)
	fwCtx.SetService(svc)

	// Step 4: Bind framework to service lifecycle
	svcStruct, err := service.NewServiceStruct(fwCtx, svc)
	if err != nil {
		fwCtx.Logger().Error(rootCtx, "❌ Service initialization failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}

	fwCtx.Logger().Info(rootCtx, "✅ Service initialized", nil)

	// Step 5: Start the main service (not under supervisor)
	//	fwCtx.Supervisor().Go(rootCtx, "service.run", func(ctx context.Context) error {
	fwCtx.Logger().Info(rootCtx, "🚀 Service is starting...", nil)
	svcStruct.Run()
	//	return nil
	//})

	// Step 6: Trap OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fwCtx.Logger().Info(rootCtx, "⏳ Waiting for any signal to shutdown...", nil)

	fwCtx.Supervisor().Go("signal.handler", func(ctx context.Context) error {
		select {
		case sig := <-sigChan:
			fwCtx.Logger().Info(ctx, "⚠️ Shutdown signal received", map[string]interface{}{"signal": sig.String()})
		case <-ctx.Done():
			fwCtx.Logger().Info(ctx, "🛑 Signal handler context canceled", nil)
			return nil
		case err := <-fwCtx.Supervisor().ErrorChannel():
			fwCtx.Logger().Error(ctx, "💥 Shutting down due error in a supervised method", map[string]interface{}{"error": err.Error()})
		}

		fwCtx.Logger().Info(ctx, "🛑 Initiating graceful shutdown...", nil)
		svcStruct.Shutdown()

		return nil
	})

	// Step 7: Wait for shutdown and exit
	fwCtx.Supervisor().WaitAndShutdown(func() {
		fwCtx.Logger().Info(rootCtx, "service shutdown phase done...", nil)
		//	svcStruct.Shutdown()
	})

	fwCtx.Logger().Info(rootCtx, "🏁 Bootstrap shutdown complete", nil)
}
