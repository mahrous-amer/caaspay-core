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

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		crashChan := make(chan error, 1)

		// 🚀 Start service in background
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

		// 🧠 Start internal health checker ONLY if config says so
		if fwContext.Config.Framework.HealthCheck.InternalHealthChecker {
			go startInternalHealthCheck(fwContext, serviceStruct, devService)
		}

		select {
		case sig := <-sigChan:
			fwContext.Logger.Info(ctx, "⚠️ Shutdown signal received", map[string]interface{}{
				"signal": sig.String(),
			})
			serviceStruct.Shutdown()

			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()

			select {
			case <-serviceStruct.Done():
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
				time.Sleep(5 * time.Second)
				continue
			} else {
				fwContext.Logger.Info(ctx, "✅ Service exited cleanly", nil)
				os.Exit(0)
			}
		}
	}
}

func startInternalHealthCheck(fwContext *framework.FrameworkContext, serviceStruct *ServiceStruct, devService Service) {
	ctx := context.Background()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			healthCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := devService.HealthCheck(healthCtx)
			cancel()

			if err != nil {
				fwContext.Logger.Error(ctx, "❌ Internal Health Check failed, shutting down service", map[string]interface{}{
					"error": err.Error(),
				})
				serviceStruct.Shutdown()
				return
			}

			serviceStruct.lifecycle.MarkReady()

		case <-serviceStruct.Done():
			return
		}
	}
}
