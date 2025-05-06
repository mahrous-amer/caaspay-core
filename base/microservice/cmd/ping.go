package main

import (
	"fmt"

	"github.com/caaspay/caaspay-core/internal/framework"
	"github.com/caaspay/caaspay-core/pkg/api"
)

// PingRequest represents the RPC request input.
type PingRequest struct {
	Message string `json:"message"`
}

// Validate implements optional input validation.
func (p *PingRequest) Validate() error {
	if p.Message == "" {
		return fmt.Errorf("message is required")
	}
	return nil
}

// PingResponse represents the RPC reply.
type PingResponse struct {
	Response string                 `json:"response"`
	Input    map[string]interface{} `json:"input"`
}

// pingService implements api.ServiceInterface
type pingService struct {
	ctx     api.FrameworkContextInterface
	healthy bool
}

// newPingService is the constructor passed to framework.Bootstrap
func newPingService(ctx api.FrameworkContextInterface) api.ServiceInterface {
	return &pingService{ctx: ctx}
}

func (s *pingService) Start() error {
	s.ctx.Logger().Info("🚀 pingService.Start called", nil)
	s.healthy = true
	return nil
}

func (s *pingService) Stop() error {
	s.ctx.Logger().Info("🛑 pingService.Stop called", nil)
	s.healthy = false
	return nil
}

func (s *pingService) HealthCheck() error {
	if s.healthy {
		s.ctx.Logger().Info("✅ pingService is healthy", nil)
		return nil
	}
	s.ctx.Logger().Error("⚠️ pingService is not healthy", nil)
	return fmt.Errorf("pingService unhealthy")
}

// RPC_Ping handles the ping RPC call.
func (s *pingService) RPC_Ping(input PingRequest) (PingResponse, error) {
	s.ctx.Logger().Info("📡 RPC_Ping invoked", nil)
	return PingResponse{
		Response: "pong",
		Input:    map[string]interface{}{"message": input.Message},
	}, nil
}

func main() {
	framework.Bootstrap(newPingService)
}
