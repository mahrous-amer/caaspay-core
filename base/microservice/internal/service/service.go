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
	}

	if err := service.registerRPCMethods(); err != nil {
		return nil, fmt.Errorf("failed to register RPC methods: %w", err)
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

		if method.Type.NumIn() == 3 && method.Type.In(1).String() == "context.Context" && method.Type.In(2).String() == "[]byte" {
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
		// Metrics and compliance tracking
		start := time.Now()
		s.frameworkCtx.Metrics.Increment(ctx, "rpc_request")
		defer s.frameworkCtx.Metrics.RecordTiming(ctx, "rpc_request_time", time.Since(start))
    defer s.frameworkCtx.Compliance.TrackEvent(ctx, "rpc_request")

		// Call the method
		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(data),
		}
		results := method.Call(args)

		// Handle errors
		if len(results) > 0 && !results[len(results)-1].IsNil() {
			return nil, results[len(results)-1].Interface().(error)
		}

		// Return the result
		if len(results) > 0 {
			return results[0].Interface().([]byte), nil
		}
		return nil, nil
	})
}

// Run starts the service and its lifecycle.
func (s *ServiceStruct) Run(serviceInstance interface{}) {
	ctx := context.Background()
	s.frameworkCtx.Logger.Info(ctx, "Starting service", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName,
	})
	s.AutoRegisterFunctions(serviceInstance)
	<-s.shutdownCh
	s.frameworkCtx.Logger.Info(ctx, "Shutting down service", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName,
	})
	s.wg.Wait()
}

// Shutdown gracefully shuts down the service.
func (s *ServiceStruct) Shutdown() {
	s.frameworkCtx.Logger.Info(context.Background(), "Shutting down service", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName,
	})
	close(s.shutdownCh)
}

// AutoRegisterFunctions auto-registers eligible framework methods.
func (s *ServiceStruct) AutoRegisterFunctions(serviceInstance interface{}) {
	svcType := reflect.TypeOf(serviceInstance)
	svcValue := reflect.ValueOf(serviceInstance)

	for i := 0; i < svcType.NumMethod(); i++ {
		method := svcType.Method(i)
		methodValue := svcValue.Method(i)
		streamName := method.Name

		if method.Type.NumIn() == 3 && method.Type.In(1).String() == "context.Context" && method.Type.In(2).String() == "[]byte" {
			s.registerRPCMethod(streamName, map[string]string{"stream": streamName})
		}

		// Poll emitter
		if method.Type.NumIn() == 2 && method.Type.In(1).String() == "context.Context" {
			s.registerEmitterPull(streamName, 10*time.Second, methodValue)
		}

		// Pull emitter
		if method.Type.NumIn() == 3 && method.Type.In(1).String() == "context.Context" && method.Type.In(2).String() == "[][]byte" {
			s.registerReceiver(streamName, 10, methodValue)
		}
	}
}

// registerEmitterPull registers a method to be called periodically.
func (s *ServiceStruct) registerEmitterPull(stream string, interval time.Duration, method reflect.Value) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-s.shutdownCh:
				return
			case <-ticker.C:
				ctx := context.Background()
				args := []reflect.Value{reflect.ValueOf(ctx)}
				start := time.Now()

				// Call the method
				results := method.Call(args)

				// Handle errors
				if len(results) > 0 && !results[len(results)-1].IsNil() {
					s.frameworkCtx.Logger.Error(ctx, "Emitter pull error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					continue
				}

				// Publish results
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

				// Call the method
				results := method.Call(args)

				// Reset the batch
				batch = batch[:0]

				// Handle errors
				if len(results) > 0 && !results[len(results)-1].IsNil() {
					s.frameworkCtx.Logger.Error(ctx, "Receiver error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					return nil, results[len(results)-1].Interface().(error)
				}

				s.frameworkCtx.Metrics.Increment(ctx, "receiver_handled")
				s.frameworkCtx.Metrics.RecordTiming(ctx, "receiver_handling_time", time.Since(start))
        s.frameworkCtx.Compliance.TrackEvent(ctx, "receiver_message") // Fixed argument mismatch

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
