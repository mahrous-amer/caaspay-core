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
}

// NewHealthServer initializes a new health server using configuration.
func NewHealthServer(service *ServiceStruct) *HealthServer {
	cfg := service.frameworkCtx.Config.Framework.HealthCheck

	mux := http.NewServeMux()
	healthServer := &HealthServer{
		service: service,
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", cfg.HTTPServerPort),
			Handler: mux,
		},
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
	go func() {
		h.service.frameworkCtx.Logger.Info(context.Background(), "🔎 Health server starting...", map[string]interface{}{
			"addr": h.server.Addr,
		})

		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			h.service.frameworkCtx.Logger.Error(context.Background(), "❌ Health server crashed", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()
}

// Stop gracefully shuts down the HTTP server.
func (h *HealthServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	h.service.frameworkCtx.Logger.Info(context.Background(), "🛑 Shutting down health server...", nil)
	if err := h.server.Shutdown(ctx); err != nil {
		h.service.frameworkCtx.Logger.Error(context.Background(), "❌ Health server shutdown error", map[string]interface{}{
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
