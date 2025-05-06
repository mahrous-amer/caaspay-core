package framework

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
		fwCtx.Logger().Error("❌ Service initialization failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}

	fwCtx.Logger().Info("✅ Service initialized", nil)

	// Step 5: Start the main service (not under supervisor)
	fwCtx.Supervisor().Go("service.run", func(ctx context.Context) error {
		fwCtx.Logger().Info("🚀 Service is starting...", nil)
		svcStruct.Run()
		return nil
	})

	// Step 6: Trap OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fwCtx.Logger().Info("⏳ Waiting for any signal to shutdown...", nil)

	fwCtx.Supervisor().Go("signal.handler", func(ctx context.Context) error {
		select {
		case sig := <-sigChan:
			fwCtx.Logger().Info("⚠️ Shutdown signal received", map[string]interface{}{"signal": sig.String()})
		case <-ctx.Done():
			fwCtx.Logger().Info("🛑 Signal handler context canceled", nil)
			return nil
		}

		fwCtx.Logger().Info("🛑 Initiating graceful shutdown...", nil)
		svcStruct.Shutdown()

		return nil
	})

	// Step 7: Wait for shutdown and exit
	fwCtx.Supervisor().WaitAndShutdown(func() {

		fwCtx.Logger().Info("⏳ Last wait for shuting down...", nil)
		// to prevent framework from getting stuck
		// perform this in a dedicated goroutine
		// let it finish, and have the last confirmation that everything is done.
		// typically won't even execute if everything shutdowns gracefully
		go func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			fwCtx.Logger().Info("⏳ Waiting for service.Done() or shutdown timeout...", nil)
			select {
			case <-fwCtx.Supervisor().Done():
				fwCtx.Logger().Info("✅ Service stopped gracefully", nil)
			case <-shutdownCtx.Done():
				fwCtx.Logger().Error("❌ Shutdown timed out. Forcing exit.", nil)
				os.Exit(1)
			}
		}()
	})

	fwCtx.Logger().Info("🏁 Bootstrap shutdown complete", nil)
}
