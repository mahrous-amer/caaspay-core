package service

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caaspay/caaspay-core/internal/framework"
)

// RunFrameworkService runs a developer-provided service inside the framework.
func RunFrameworkService(devService Service) {
	for {
		ctx, cancel := context.WithCancel(context.Background())

		fwContext, err := framework.NewFrameworkContext()
		if err != nil {
			panic(err)
		}

		serviceStruct, err := NewServiceStruct(fwContext, devService)
		if err != nil {
			fwContext.Logger.Error(ctx, "❌ Service initialization failed", map[string]interface{}{
				"error": err.Error(),
			})
			os.Exit(1)
		}

		// OS signal and crash channel
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		crashChan := make(chan error, 1)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					fwContext.Logger.Error(ctx, "🔥 Service panic captured", map[string]interface{}{
						"panic": r,
					})
					crashChan <- fmt.Errorf("service panicked: %v", r)
				}
			}()
			serviceStruct.Run()
			crashChan <- nil // Normal exit
		}()

		select {
		case sig := <-sigChan:
			fwContext.Logger.Info(ctx, "⚠️ Shutdown signal received", map[string]interface{}{
				"signal": sig.String(),
			})
			serviceStruct.Shutdown()

			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()

			select {
			case <-serviceStruct.Done(): // 💥 wait for real shutdown
				fwContext.Logger.Info(ctx, "✅ Service shutdown complete", nil)
			case <-shutdownCtx.Done():
				fwContext.Logger.Error(ctx, "❌ Shutdown timeout. Forcing exit.", nil)
				os.Exit(1)
			}

			os.Exit(0)

		case err := <-crashChan:
			if err != nil {
				fwContext.Logger.Error(ctx, "💥 Service crashed, restarting...", map[string]interface{}{
					"error": err.Error(),
				})
				cancel()
				time.Sleep(5 * time.Second) // backoff before restart
				continue
			} else {
				fwContext.Logger.Info(ctx, "✅ Service exited cleanly", nil)
				os.Exit(0)
			}
		}
	}
}
