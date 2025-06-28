package main

import (
	"context"
	"encoding/json"
	"fmt"
	//	"io"
	//"math/rand"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caaspay/caaspay-core/internal/framework"
	"github.com/caaspay/caaspay-core/pkg/api"
	"github.com/caaspay/caaspay-core/pkg/common/transport"
	"github.com/caaspay/caaspay-core/pkg/structs"
)

//type PingRequest struct {
//	Message string `json:"message"`
//}
//
//func (p *PingRequest) Validate() error {
//	if p.Message == "" {
//		return fmt.Errorf("message is required")
//	}
//	return nil
//}
//
//type PingResponse struct {
//	Response string                 `json:"response"`
//	Input    map[string]interface{} `json:"input"`
//}

type HeartbeatMessage struct {
	Node   string `json:"node"`
	Uptime int64  `json:"uptime"`
	Status string `json:"status"`
}

type PushlogMessage struct {
	Ts     string `json:"ts"`
	Uptime string `json:"uptime"`
	Note   string `json:"note"`
}

type MyRequest struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type MyResponse struct {
	Status string `json:"status"`
	ID     string `json:"id"`
}

type pingService struct {
	ctx         api.FrameworkContextInterface
	healthy     bool
	pingCount   int32
	pingReq     int32
	callerCount int32
	uptimeStart time.Time
	emitChan    chan *transport.TransportMessage
	pingLock    sync.Mutex
	pushlogChan chan *transport.TransportMessage
}

func newPingService(ctx api.FrameworkContextInterface) api.ServiceInterface {
	s := &pingService{
		ctx:         ctx,
		uptimeStart: time.Now(),
		emitChan:    make(chan *transport.TransportMessage, 1),
	}

	return s
}

func (s *pingService) Start(ctx context.Context) error {
	s.ctx.Logger().Info(ctx, "🚀 pingService.Start called", nil)
	s.healthy = true
	return nil
}

func (s *pingService) Stop(ctx context.Context) error {
	s.ctx.Logger().Info(ctx, "🛑 pingService.Stop called", nil)
	s.healthy = false
	close(s.emitChan)
	return nil
}

func (s *pingService) HealthCheck(ctx context.Context) error {
	c := atomic.AddInt32(&s.callerCount, 1)
	//s.ctx.Supervisor().GoLoop("call_rpc_healthcheck", func(ctx context.Context) (time.Duration, error) {
	n := atomic.AddInt32(&s.pingReq, 1)
	stream := s.ctx.BuildRPCStreamName("Ping")
	req := structs.PingRequest{Message: strconv.Itoa(int(c)) + " CALLER " + strconv.Itoa(int(n))}
	var resp structs.PingResponse
	if err := s.ctx.RequestRPC(stream, req, &resp, 50*time.Second); err != nil {
		//	return 2 * time.Second, fmt.Errorf("rpc ping request failed: %w", err)
	}
	s.ctx.Logger().Info(ctx, "✅ ✅✅✅✅✅✅✅✅Healthcheck RPC_Ping passed", map[string]interface{}{
		"response": resp.Response,
		"echo":     resp.Input,
	})
	if err := s.ctx.RequestRPC(stream, req, &resp, 50*time.Second); err != nil {
		//	return 2 * time.Second, fmt.Errorf("rpc ping request failed: %w", err)
	}
	s.ctx.Logger().Info(ctx, "✅ ✅✅✅✅✅✅✅✅Healthcheck2 RPC_Ping passed", map[string]interface{}{
		"response": resp.Response,
		"echo":     resp.Input,
	})
	//sleepDuration := time.Duration(rand.Intn(4)+1) * time.Second
	log := map[string]any{"note": "triggered", "ts": "TTTTTTTT " + strconv.Itoa(int(n))}
	payload, _ := json.Marshal(log)

	msg2 := transport.NewTransportMessage(s.ctx.ServiceName(), "pushlog", payload)
	msg2.Trace["source"] = "manual_trigger"
	//select {
	//case s.pushlogChan <- msg2:
	//	// success
	//case <-ctx.Done():
	//	return nil, 0, ctx.Err()
	//}
	s.pushlogChan <- msg2
	//	return sleepDuration, nil
	//})

	httpreq := MyRequest{
		Name: "John",
		Age:  30,
	}

	var httpresp MyResponse

	err := s.ctx.RequestHTTP(ctx, "GET", "https://api.caaspay.com/status", httpreq, map[string]string{
		"Authorization": "Bearer my-token",
	}, &httpresp)

	if err != nil {
		s.ctx.Logger().Fatal(ctx, "❌ RequestHTTP failed", map[string]interface{}{"error": err.Error()})
	} else {
		s.ctx.Logger().Info(ctx, "✅ RequestHTTP OK", map[string]interface{}{"response": httpresp})
	}

	hh := map[string]string{
		"type": "test_RAW",
	}
	bo, _ := json.Marshal(hh)
	rrr, r2, _ := s.ctx.RequestHTTPRaw(ctx, "POST", "https://api.caaspay.com/webhook", bo, nil)
	s.ctx.Logger().Info(ctx, "✅✅✅✅✅✅ RequestHTTP OK", map[string]interface{}{"response": rrr, "body": r2})

	return nil
}

func (s *pingService) RPC_Ping(ctx context.Context, input structs.PingRequest) (structs.PingResponse, error) {
	s.pingLock.Lock()
	defer s.pingLock.Unlock()
	n := atomic.AddInt32(&s.pingCount, 1)
	s.ctx.Logger().Info(ctx, fmt.Sprintf("📡 RPC_Ping invoked %d", n), nil)
	return structs.PingResponse{
		Response: fmt.Sprintf("pong %d", n),
		Input:    map[string]interface{}{"message": input.Message},
	}, nil
}

func (s *pingService) EmitterPull_Heartbeat(ctx context.Context) ([]*transport.TransportMessage, time.Duration, error) {
	//select {
	//case <-ctx.Done():
	//	return nil, 0, ctx.Err()
	//default:
	//}

	heartbeat := HeartbeatMessage{
		Node:   s.ctx.ServiceName(),
		Uptime: int64(time.Since(s.uptimeStart).Seconds()),
		Status: "alive",
	}
	data, _ := json.Marshal(heartbeat)
	msg := transport.NewTransportMessage(s.ctx.ServiceName(), "ping.heartbeat", data)
	msg.Trace["source"] = "emitter_pull"

	//log := map[string]any{"note": "triggered", "ts": "TTTTTTTT",}
	//payload, _ := json.Marshal(log)

	//msg2 := transport.NewTransportMessage(s.ctx.ServiceName(), "pushlog", payload)
	//msg2.Trace["source"] = "manual_trigger"
	//select {
	//case s.pushlogChan <- msg2:
	//	// success
	//case <-ctx.Done():
	//	return nil, 0, ctx.Err()
	//}
	//s.pushlogChan <- msg2
	return []*transport.TransportMessage{msg, msg, msg, msg, msg, msg}, 10 * time.Millisecond, nil
}

func (s *pingService) EmitterChannel_PushLog(ctx context.Context, ch chan *transport.TransportMessage) {
	s.pushlogChan = ch
}

//func (s *pingService) EmitterChannel_PushLog(ctx context.Context, ch chan *transport.TransportMessage) {
//	ticker := time.NewTicker(1 * time.Millisecond)
//	defer ticker.Stop()
//
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case ts := <-ticker.C:
//			log := map[string]interface{}{
//				"ts":     ts,
//				"note":   "periodic log",
//				"uptime": time.Since(s.uptimeStart).String(),
//			}
//			data, _ := json.Marshal(log)
//			msg := transport.NewTransportMessage(s.ctx.ServiceName(), "ping.push_log", data)
//			msg.Trace["source"] = "emitter_channel"
//			ch <- msg
//		}
//	}
//}

func (s *pingService) Receiver_example__service_Heartbeat(ctx context.Context, msg *HeartbeatMessage) error {
	//s.ctx.Logger().Info(ctx, "✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅ Heartbeat received", map[string]interface{}{
	//	"node":   msg.Node,
	//	"status": msg.Status,
	//	"uptime": msg.Uptime,
	//})
	return nil
}

func (s *pingService) Receiver_example__service_Pushlog(ctx context.Context, msg *PushlogMessage) error {
	s.ctx.Logger().Info(ctx, "✅ Pushlog received", map[string]interface{}{
		"note":   msg.Note,
		"ts":     msg.Ts,
		"uptime": msg.Uptime,
	})
	if false {
		return errors.New("test error from Pushlog receiver")
	}
	return nil
}

func main() {
	framework.Bootstrap(newPingService)
}
