package service

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/caaspay/caaspay-core/internal/framework"
	"github.com/caaspay/caaspay-core/internal/transport"
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
	healthServer    *HealthServer
	lifecycle       *ServiceLifecycle
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
		lifecycle:       NewServiceLifecycle(),
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

// Run starts the service and its lifecycle.
func (s *ServiceStruct) Run() {
	defer close(s.doneCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.frameworkCtx.Logger.Info(ctx, "Starting service", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName,
	})

	s.AutoRegisterFunctions(s.serviceInstance)

	// Phase 1: Mark as started
	s.lifecycle.MarkStarted()

	// If HTTP health server is enabled, start it
	if s.frameworkCtx.Config.Framework.HealthCheck.HTTPServerEnabled {
		s.healthServer = NewHealthServer(s)
		s.healthServer.Start()
	}

	// Step 1️⃣: Call developer's Start()
	if svc, ok := s.serviceInstance.(Service); ok {
		if err := svc.Start(ctx); err != nil {
			s.frameworkCtx.Logger.Error(ctx, "❌ Service Start failed", map[string]interface{}{
				"error": err.Error(),
			})
			return
		}
	}

	// Step 2️⃣: Immediately call HealthCheck()
	if svc, ok := s.serviceInstance.(Service); ok {
		if err := svc.HealthCheck(ctx); err != nil {
			s.frameworkCtx.Logger.Error(ctx, "❌ HealthCheck failed", map[string]interface{}{
				"error": err.Error(),
			})
			// Optionally shutdown early
			return
		}
	}

	// Phase 3: Mark ready
	s.lifecycle.MarkReady()
	s.frameworkCtx.Logger.Info(ctx, "✅ Service marked as ready", nil)

	// 3️⃣: Now wait for shutdown
	<-s.shutdownCh
	cancel()

	s.frameworkCtx.Logger.Info(ctx, "🛑 Shutting down service...", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName,
	})

	// Step 4️⃣: Call Stop()
	if svc, ok := s.serviceInstance.(Service); ok {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer stopCancel()
		if err := svc.Stop(stopCtx); err != nil {
			s.frameworkCtx.Logger.Error(ctx, "❌ Service Stop failed", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}

	// Wait for all background workers
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		s.wg.Wait()
	}()

	select {
	case <-waitDone:
		s.frameworkCtx.Logger.Info(ctx, "✅ Shutdown complete", nil)
	case <-time.After(20 * time.Second):
		s.frameworkCtx.Logger.Error(ctx, "❌ Shutdown timed out", nil)
	}
}

// Shutdown gracefully signals the service to stop.
func (s *ServiceStruct) Shutdown() {
	// Shutdown health server if running
	if s.healthServer != nil {
		s.healthServer.Stop()
	}
	s.lifecycle.MarkShutdown()
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

	methodType := method.Type()
	if methodType.NumIn() != 2 || methodType.In(0).String() != "context.Context" {
		return fmt.Errorf("method %s must have signature func(context.Context, T) (T2, error)", methodName)
	}

	argType := methodType.In(1)
	retErrType := methodType.Out(1)
	if retErrType != reflect.TypeOf((*error)(nil)).Elem() {
		return fmt.Errorf("method %s must return error as second value", methodName)
	}

	handler := func(ctx context.Context, data []byte) ([]byte, error) {
		start := time.Now()
		s.frameworkCtx.Metrics.Increment(ctx, "rpc_request")
		defer s.frameworkCtx.Metrics.RecordTiming(ctx, "rpc_request_time", time.Since(start))
		defer s.frameworkCtx.Compliance.TrackEvent(ctx, "rpc_request")

		// Decode transport message
		var msg transport.TransportMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			s.frameworkCtx.Logger.Error(ctx, "Failed to decode TransportMessage", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}

		// Decode Args
		argPtr := reflect.New(argType)
		if err := json.Unmarshal(msg.Args, argPtr.Interface()); err != nil {
			s.frameworkCtx.Logger.Error(ctx, "Failed to unmarshal args", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}

		// Call Validate if exists
		if validator, ok := argPtr.Interface().(interface{ Validate() error }); ok {
			if err := validator.Validate(); err != nil {
				s.frameworkCtx.Logger.Error(ctx, "Validation failed", map[string]interface{}{
					"error": err.Error(),
				})
				return nil, err
			}
		}

		// Call actual method
		results := method.Call([]reflect.Value{reflect.ValueOf(ctx), argPtr.Elem()})

		// Handle error
		if errVal := results[1]; !errVal.IsNil() {
			return nil, errVal.Interface().(error)
		}

		// Encode response
		response := results[0].Interface()
		rawResponse, err := json.Marshal(response)
		if err != nil {
			return nil, fmt.Errorf("failed to encode response: %w", err)
		}

		// Send reply if RPC
		if msg.ReplyTo != "" {
			reply := transport.NewTransportMessage(s.frameworkCtx.ServiceName, methodName, rawResponse)
			reply.MessageID = msg.MessageID
			reply.Trace = msg.Trace
			reply.Stash = msg.Stash

			replyBytes, err := json.Marshal(reply)
			if err != nil {
				s.frameworkCtx.Logger.Error(ctx, "Failed to marshal reply message", map[string]interface{}{
					"error": err.Error(),
				})
				return nil, err
			}
			if err := s.frameworkCtx.Transport.Publish(ctx, msg.ReplyTo, replyBytes); err != nil {
				s.frameworkCtx.Logger.Error(ctx, "Failed to publish RPC reply", map[string]interface{}{
					"error": err.Error(),
				})
				return nil, err
			}
		}
		return nil, nil
	}

	return s.frameworkCtx.Transport.Subscribe(stream, handler)
}

// registerReceiver registers a method to handle incoming messages.
func (s *ServiceStruct) registerReceiver(stream string, batchSize int, method reflect.Value) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		batch := make([]*transport.TransportMessage, 0, batchSize)

		handler := func(ctx context.Context, raw []byte) ([]byte, error) {
			msg := &transport.TransportMessage{}
			if err := json.Unmarshal(raw, msg); err != nil {
				s.frameworkCtx.Logger.Error(ctx, "Receiver: failed to unmarshal message", map[string]interface{}{
					"stream": stream,
					"error":  err.Error(),
				})
				return nil, err
			}
			batch = append(batch, msg)

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
		}

		if err := s.frameworkCtx.Transport.Subscribe(stream, handler); err != nil {
			s.frameworkCtx.Logger.Error(context.Background(), "Failed to subscribe to stream", map[string]interface{}{
				"stream": stream,
				"error":  err.Error(),
			})
		}
	}()
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
					if rawArgs, ok := results[0].Interface().(json.RawMessage); ok {
						tmsg := transport.NewTransportMessage(s.frameworkCtx.ServiceName, stream, rawArgs)
						tmsg.Who = s.frameworkCtx.ServiceName
						tmsg.Trace = map[string]string{"event": "emitter_pull"}

						encoded, err := tmsg.Encode()
						if err != nil {
							s.frameworkCtx.Logger.Error(ctx, "Failed to marshal transport message", map[string]interface{}{
								"stream": stream,
								"error":  err.Error(),
							})
							continue
						}

						s.frameworkCtx.Metrics.Increment(ctx, "emitter_pulled")
						s.frameworkCtx.Metrics.RecordTiming(ctx, "emitter_pulling_time", time.Since(start))
						s.frameworkCtx.Compliance.TrackEvent(ctx, "emitter_pull")

						if err := s.frameworkCtx.Transport.Publish(ctx, stream, encoded); err != nil {
							s.frameworkCtx.Logger.Error(ctx, "Failed to publish message", map[string]interface{}{
								"stream": stream,
								"error":  err.Error(),
							})
						}
					} else {
						s.frameworkCtx.Logger.Error(ctx, "Emitter pull returned non-json.RawMessage value", map[string]interface{}{
							"stream": stream,
						})
					}
				}
			}
		}
	}()
}

// registerEmitterPoll registers a method to be called periodically and publishes all returned messages.
func (s *ServiceStruct) registerEmitterPoll(stream string, interval time.Duration, method reflect.Value) {
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
					s.frameworkCtx.Logger.Error(ctx, "Emitter poll error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					continue
				}

				if len(results) > 0 {
					items, ok := results[0].Interface().([][]byte)
					if !ok {
						s.frameworkCtx.Logger.Error(ctx, "Emitter poll returned unexpected type", nil)
						continue
					}

					for _, raw := range items {
						msg := transport.NewTransportMessage(s.frameworkCtx.ServiceName, stream, raw)
						msg.Who = s.frameworkCtx.ServiceName
						msg.Trace = map[string]string{"source": "emitter_poll"}

						encoded, err := json.Marshal(msg)
						if err != nil {
							s.frameworkCtx.Logger.Error(ctx, "Failed to encode message", map[string]interface{}{
								"error": err.Error(),
							})
							continue
						}

						s.frameworkCtx.Metrics.Increment(ctx, "emitter_polled")
						s.frameworkCtx.Metrics.RecordTiming(ctx, "emitter_polling_time", time.Since(start))
						s.frameworkCtx.Compliance.TrackEvent(ctx, "emitter_poll")

						if err := s.frameworkCtx.Transport.Publish(ctx, stream, encoded); err != nil {
							s.frameworkCtx.Logger.Error(ctx, "Failed to publish emitter message", map[string]interface{}{
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
