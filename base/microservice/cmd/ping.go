package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	ctx       api.FrameworkContextInterface
	healthy   bool
	pingCount int32
	pingReq   int32
	pingLock  sync.Mutex
	reqLock   sync.Mutex
}

// newPingService is the constructor passed to framework.Bootstrap
func newPingService(ctx api.FrameworkContextInterface) api.ServiceInterface {
	return &pingService{ctx: ctx, pingCount: 0}
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
	//	if s.healthy {
	//		s.ctx.Logger().Info("✅ pingService is healthy", nil)
	//		return nil
	//	}
	//	s.ctx.Logger().Error("⚠️ pingService is not healthy", nil)
	//	return fmt.Errorf("pingService unhealthy")
	//  stream := s.ctx.Transport().BuildStreamName(api.StreamConfig{
	//		Type:    api.StreamTypeRPC,
	//		Service: s.ctx.ServiceName(),
	//		Method:  "Ping",
	//	})
	// Prepare request
	//	var count int32

	s.ctx.Supervisor().GoLoop("call_rpc_healthcheck", func(ctx context.Context) error {
		s.reqLock.Lock()
		defer s.reqLock.Unlock()
		n := atomic.AddInt32(&s.pingReq, 1)

		stream := s.ctx.BuildRPCStreamName("Ping")
		req := PingRequest{Message: "CALLER " + strconv.Itoa(int(n))}

		var resp PingResponse
		if err := s.ctx.RequestRPC(stream, req, &resp, 5*time.Second); err != nil {
			return fmt.Errorf("rpc ping request failed: %w", err)
		}

		s.ctx.Logger().Info("Ping response received", map[string]interface{}{
			"response": resp.Response,
		})
		s.ctx.Logger().Info("✅ Healthcheck RPC_Ping passed", map[string]interface{}{
			"response": resp.Response,
			"echo":     resp.Input,
		})

		time.Sleep(10 * time.Millisecond) // optional: prevent fast looping
		return nil
	})

	return nil
}

// RPC_Ping handles the ping RPC call.
func (s *pingService) RPC_Ping(input PingRequest) (PingResponse, error) {
	s.pingLock.Lock()
	defer s.pingLock.Unlock()

	n := atomic.AddInt32(&s.pingCount, 1)
	s.ctx.Logger().Info(fmt.Sprintf("📡 RPC_Ping invoked %d", n), nil)

	return PingResponse{
		Response: fmt.Sprintf("pong %d", n),
		Input:    map[string]interface{}{"message": input.Message},
	}, nil
}

func main() {
	framework.Bootstrap(newPingService)
}
