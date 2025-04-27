package service

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/caaspay/caaspay-core/internal/framework"
)

// Service defines the interface for core service operations.
type Service interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	HealthCheck(ctx context.Context) error
}

// NewService creates a new Service implementation.
func NewService() Service {
	return &serviceImpl{}
}

type serviceImpl struct{}

func (s *serviceImpl) Start(ctx context.Context) error {
	return nil
}

func (s *serviceImpl) Stop(ctx context.Context) error {
	return nil
}

func (s *serviceImpl) HealthCheck(ctx context.Context) error {
	return nil
}

// ServiceStruct represents the core service structure with FrameworkContext.
type ServiceStruct struct {
	frameworkCtx    *framework.FrameworkContext
	serviceInstance interface{}
	shutdownCh      chan struct{}
	doneCh          chan struct{}
	wg              sync.WaitGroup
}

// NewServiceStruct initializes a new service instance with FrameworkContext.
func NewServiceStruct(fwCtx *framework.FrameworkContext, serviceInstance interface{}) (*ServiceStruct, error) {
	if serviceInstance == nil {
		return nil, fmt.Errorf("service instance cannot be nil")
	}

	service := &ServiceStruct{
		frameworkCtx:    fwCtx,
		serviceInstance: serviceInstance,
		shutdownCh:      make(chan struct{}),
		doneCh:          make(chan struct{}),
	}

	return service, nil
}

// registerRPCMethods registers all RPC methods from the service instance.
func (s *ServiceStruct) registerRPCMethods() error {
	serviceType := reflect.TypeOf(s.serviceInstance)
	for i := 0; i < serviceType.NumMethod(); i++ {
		method := serviceType.Method(i)
		if !method.IsExported() {
			continue
		}

		if method.Type.NumIn() == 3 &&
			method.Type.In(1).String() == "context.Context" &&
			method.Type.In(2).String() == "[]byte" {
			stream := method.Name
			if err := s.registerRPCMethod(stream, map[string]string{"stream": stream}); err != nil {
				return fmt.Errorf("failed to register RPC method %s: %w", method.Name, err)
			}
		}
	}
	return nil
}

// registerRPCMethod registers a single RPC method with the transport.
func (s *ServiceStruct) registerRPCMethod(methodName string, config map[string]string) error {
	method := reflect.ValueOf(s.serviceInstance).MethodByName(methodName)
	if !method.IsValid() {
		return fmt.Errorf("method %s not found", methodName)
	}

	stream := config["stream"]
	if stream == "" {
		return fmt.Errorf("stream not specified for method %s", methodName)
	}

	return s.frameworkCtx.Transport.Subscribe(stream, func(ctx context.Context, data []byte) ([]byte, error) {
		start := time.Now()
		s.frameworkCtx.Metrics.Increment(ctx, "rpc_request")
		defer s.frameworkCtx.Metrics.RecordTiming(ctx, "rpc_request_time", time.Since(start))
		defer s.frameworkCtx.Compliance.TrackEvent(ctx, "rpc_request")

		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(data),
		}
		results := method.Call(args)

		if len(results) > 0 && !results[len(results)-1].IsNil() {
			return nil, results[len(results)-1].Interface().(error)
		}

		if len(results) > 0 {
			return results[0].Interface().([]byte), nil
		}
		return nil, nil
	})
}

// Run starts the service and its lifecycle.
// Run starts the service and its lifecycle.
func (s *ServiceStruct) Run() {
	defer close(s.doneCh) // 🛑 Critical: close doneCh when Run finishes

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.frameworkCtx.Logger.Info(ctx, "Starting service", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName,
	})

	s.AutoRegisterFunctions(s.serviceInstance)

	// Wait until shutdown signal is received
	<-s.shutdownCh
	cancel()

	s.frameworkCtx.Logger.Info(ctx, "🛑 Shutting down service...", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName,
	})

	// Start a background goroutine to wait for all workers
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		s.wg.Wait()
	}()

	// Wait for either all workers done or timeout
	select {
	case <-waitDone:
		s.frameworkCtx.Logger.Info(ctx, "✅ Shutdown complete", nil)
	case <-time.After(20 * time.Second): // fallback timeout
		s.frameworkCtx.Logger.Error(ctx, "❌ Shutdown timed out", nil)
	}
}

// Shutdown gracefully signals the service to stop.
func (s *ServiceStruct) Shutdown() {
	select {
	case <-s.shutdownCh:
		// already closed
	default:
		close(s.shutdownCh)
	}
}

func (s *ServiceStruct) Done() <-chan struct{} {
	return s.doneCh
}

// AutoRegisterFunctions auto-registers eligible framework methods based on naming conventions.
func (s *ServiceStruct) AutoRegisterFunctions(serviceInstance interface{}) {
	svcType := reflect.TypeOf(serviceInstance)
	svcValue := reflect.ValueOf(serviceInstance)

	for i := 0; i < svcType.NumMethod(); i++ {
		method := svcType.Method(i)
		methodValue := svcValue.Method(i)
		methodName := method.Name

		switch {
		case method.Type.NumIn() == 3 &&
			method.Type.In(1).String() == "context.Context" &&
			method.Type.In(2).String() == "[]byte" &&
			hasPrefix(methodName, "RPC_"):

			stream := trimPrefix(methodName, "RPC_")
			s.registerRPCMethod(stream, map[string]string{"stream": stream})

		case method.Type.NumIn() == 2 &&
			method.Type.In(1).String() == "context.Context" &&
			hasPrefix(methodName, "Emitter_"):

			stream := trimPrefix(methodName, "Emitter_")
			s.registerEmitterPull(stream, 10*time.Second, methodValue)

		case method.Type.NumIn() == 3 &&
			method.Type.In(1).String() == "context.Context" &&
			method.Type.In(2).String() == "[][]byte" &&
			hasPrefix(methodName, "Receiver_"):

			stream := trimPrefix(methodName, "Receiver_")
			s.registerReceiver(stream, 10, methodValue)
		}
	}
}

// Helper functions for string prefix matching
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func trimPrefix(s, prefix string) string {
	if hasPrefix(s, prefix) {
		return s[len(prefix):]
	}
	return s
}

// registerEmitterPull registers a method to be called periodically.
func (s *ServiceStruct) registerEmitterPull(stream string, interval time.Duration, method reflect.Value) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-s.shutdownCh:
				return
			case <-ticker.C:
				args := []reflect.Value{reflect.ValueOf(ctx)}
				start := time.Now()

				results := method.Call(args)

				if len(results) > 0 && !results[len(results)-1].IsNil() {
					s.frameworkCtx.Logger.Error(ctx, "Emitter pull error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					continue
				}

				if len(results) > 0 {
					if data, ok := results[0].Interface().([]byte); ok {
						s.frameworkCtx.Metrics.Increment(ctx, "emitter_pulled")
						s.frameworkCtx.Metrics.RecordTiming(ctx, "emitter_pulling_time", time.Since(start))
						s.frameworkCtx.Compliance.TrackEvent(ctx, "emitter_pull")

						if err := s.frameworkCtx.Transport.Publish(ctx, stream, data); err != nil {
							s.frameworkCtx.Logger.Error(ctx, "Failed to publish message", map[string]interface{}{
								"stream": stream,
								"error":  err.Error(),
							})
						}
					}
				}
			}
		}
	}()
}

// registerReceiver registers a method to handle incoming messages.
func (s *ServiceStruct) registerReceiver(stream string, batchSize int, method reflect.Value) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		batch := make([][]byte, 0, batchSize)

		if err := s.frameworkCtx.Transport.Subscribe(stream, func(ctx context.Context, data []byte) ([]byte, error) {
			batch = append(batch, data)

			if len(batch) >= batchSize {
				args := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(batch)}
				start := time.Now()

				results := method.Call(args)

				batch = batch[:0]

				if len(results) > 0 && !results[len(results)-1].IsNil() {
					s.frameworkCtx.Logger.Error(ctx, "Receiver error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					return nil, results[len(results)-1].Interface().(error)
				}

				s.frameworkCtx.Metrics.Increment(ctx, "receiver_handled")
				s.frameworkCtx.Metrics.RecordTiming(ctx, "receiver_handling_time", time.Since(start))
				s.frameworkCtx.Compliance.TrackEvent(ctx, "receiver_message")
			}
			return nil, nil
		}); err != nil {
			s.frameworkCtx.Logger.Error(context.Background(), "Failed to subscribe to stream", map[string]interface{}{
				"stream": stream,
				"error":  err.Error(),
			})
		}
	}()
}
