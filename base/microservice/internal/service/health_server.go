package service

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HealthServer serves health, readiness, liveness, and optional metrics endpoints.
type HealthServer struct {
	service *ServiceStruct
	server  *http.Server
	ctx     context.Context
}

// NewHealthServer initializes a new health server using configuration.
func NewHealthServer(service *ServiceStruct) *HealthServer {
	cfg := service.frameworkCtx.Config().Framework.HealthCheck
	ctx := service.frameworkCtx.Context()

	mux := http.NewServeMux()
	healthServer := &HealthServer{
		service: service,
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", cfg.HTTPServerPort),
			Handler: mux,
		},
		ctx: ctx,
	}

	// Attach handlers based on config
	mux.HandleFunc(cfg.HTTPServerHealthRoute, healthServer.handleHealthz)
	mux.HandleFunc(cfg.HTTPServerReadyRoute, healthServer.handleReadyz)
	mux.HandleFunc(cfg.HTTPServerLiveRoute, healthServer.handleLivez)

	// Optional: expose /metrics endpoint if enabled
	if cfg.ExposeMetricsEndpoint {
		mux.Handle(cfg.MetricsRoute, promhttp.Handler())
	}

	return healthServer
}

// Start launches the HTTP server in background.
func (h *HealthServer) Start() {
	h.service.frameworkCtx.Supervisor().Go("health_server", func(ctx context.Context) error {
		h.service.frameworkCtx.Logger().Info(ctx, "🔎 Health server starting...", map[string]interface{}{
			"addr": h.server.Addr,
		})

		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			h.service.frameworkCtx.Logger().Error(ctx, "❌ Health server crashed", map[string]interface{}{
				"error": err.Error(),
			})
			return err
		}
		return nil
	})
}

// Stop gracefully shuts down the HTTP server.
func (h *HealthServer) Stop() {
	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()

	h.service.frameworkCtx.Logger().Info(h.ctx, "🛑 Shutting down health server...", nil)
	if err := h.server.Shutdown(ctx); err != nil {
		h.service.frameworkCtx.Logger().Error(h.ctx, "❌ Health server shutdown error", map[string]interface{}{
			"error": err.Error(),
		})
	}
}

// handleHealthz returns 200 if service is started and not shutting down.
func (h *HealthServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if !h.service.lifecycle.IsStarted() || h.service.lifecycle.IsShutdown() {
		http.Error(w, "Service not healthy", http.StatusServiceUnavailable)
		return
	}

	// Check Redis health
	if !h.service.frameworkCtx.IsHealthy() {
		http.Error(w, "Framework Redis not healthy", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handleReadyz returns 200 if service is ready, else 503.
func (h *HealthServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !h.service.lifecycle.IsReady() {
		http.Error(w, "Service not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handleLivez always returns 200 unless service is marked shutdown.
func (h *HealthServer) handleLivez(w http.ResponseWriter, r *http.Request) {
	if h.service.lifecycle.IsShutdown() {
		http.Error(w, "Service shutting down", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
