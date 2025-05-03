package main

import (
	"context"
	"fmt"

	"github.com/caaspay/caaspay-core/internal/framework"
	"github.com/caaspay/caaspay-core/pkg/api"
)

// Optional: Add validation if needed
func (p *PingRequest) Validate() error {
	if p.Message == "" {
		return fmt.Errorf("message is required")
	}
	return nil
}

type pingService struct {
	ctx     api.FrameworkContextInterface
	healthy bool
}

func newPingService(ctx api.FrameworkContextInterface) api.ServiceInterface {
	return &pingService{ctx: ctx}
}

func (s *pingService) Start(ctx context.Context) error {
	s.ctx.Logger().Info(ctx, "🚀 pingService.Start called", nil)
	s.healthy = true
	return nil
}

func (s *pingService) Stop(ctx context.Context) error {
	s.ctx.Logger().Info(ctx, "🛑 pingService.Stop called", nil)
	s.healthy = false
	return nil
}

func (s *pingService) HealthCheck(ctx context.Context) error {
	if s.healthy {
		s.ctx.Logger().Info(ctx, "✅ pingService is healthy", nil)
		return nil
	}
	s.ctx.Logger().Error(ctx, "⚠️ pingService is not healthy", nil)
	return fmt.Errorf("pingService unhealthy")
}

type PingRequest struct {
	Message string `json:"message"`
}

type PingResponse struct {
	Response string                 `json:"response"`
	Input    map[string]interface{} `json:"input"`
}

func (s *pingService) RPC_Ping(ctx context.Context, input PingRequest) (PingResponse, error) {
	s.ctx.Logger().Info(ctx, "📡 RPC_Ping invoked", nil)
	return PingResponse{
		Response: "pong",
		Input:    map[string]interface{}{"message": input.Message},
	}, nil
}

func main() {
	framework.Bootstrap(newPingService)
}
