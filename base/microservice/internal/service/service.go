package service

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/caaspay/caaspay-core/internal/transport"
	"github.com/caaspay/caaspay-core/pkg/api"
)

// ServiceStruct represents the core service structure with FrameworkContext.
type ServiceStruct struct {
	frameworkCtx    api.FrameworkContextInterface
	serviceInstance api.ServiceInterface
	supervisor      api.SupervisorInterface
	healthServer    *HealthServer
	lifecycle       *ServiceLifecycle
}

// NewServiceStruct initializes a new service instance with FrameworkContext.
func NewServiceStruct(fwCtx api.FrameworkContextInterface, serviceInstance api.ServiceInterface) (*ServiceStruct, error) {
	if serviceInstance == nil {
		return nil, fmt.Errorf("service instance cannot be nil")
	}

	return &ServiceStruct{
		frameworkCtx:    fwCtx,
		serviceInstance: serviceInstance,
		supervisor:      fwCtx.Supervisor(),
		lifecycle:       NewServiceLifecycle(),
	}, nil
}

// Run starts the service and its lifecycle.
func (s *ServiceStruct) Run() {

	s.frameworkCtx.Logger().Info("Starting service", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName(),
	})

	s.autoRegisterFunctions(s.serviceInstance)
	s.lifecycle.MarkStarted()

	if s.frameworkCtx.Config().Framework.HealthCheck.HTTPServerEnabled {
		s.healthServer = NewHealthServer(s)
		s.healthServer.Start()
	}

	if err := s.serviceInstance.Start(); err != nil {
		s.frameworkCtx.Logger().Error("❌ Service Start failed", map[string]interface{}{"error": err.Error()})
		return
	}
	if err := s.serviceInstance.HealthCheck(); err != nil {
		s.frameworkCtx.Logger().Error("❌ HealthCheck failed", map[string]interface{}{"error": err.Error()})
		return
	}

	s.lifecycle.MarkReady()
	s.frameworkCtx.Logger().Info("✅ Service marked as ready", nil)

	// Launch the supervisor's shutdown watcher

	//go s.supervisor.WaitAndShutdown(func() {
	//	s.Shutdown()
	//})

	// Block until all supervised goroutines finish
	//<-s.supervisor.Done()
	// ✅ Block until shutdown requested via fwCtx.Context
	<-s.frameworkCtx.Context().Done()

	s.frameworkCtx.Logger().Info("🛑 Service.Run exiting due to context cancellation", nil)
}

// Shutdown gracefully signals the service to stop.
func (s *ServiceStruct) Shutdown() {
  s.frameworkCtx.Logger().Info("Service shutting down",nil)
	if s.healthServer != nil {
		s.healthServer.Stop()
	}

	s.lifecycle.MarkShutdown()
	s.supervisor.Shutdown()

	// Wait for all supervised goroutines to finish
	//<-s.supervisor.Done()

	s.supervisor.StopAll()

	if err := s.serviceInstance.Stop(); err != nil {
		s.frameworkCtx.Logger().Error("❌ Service Stop failed", map[string]interface{}{"error": err.Error()})
	}
}

func (s *ServiceStruct) Done() <-chan struct{} {
	return s.supervisor.Done()
}

func (s *ServiceStruct) autoRegisterFunctions(serviceInstance interface{}) {
	svcType := reflect.TypeOf(serviceInstance)
	svcValue := reflect.ValueOf(serviceInstance)

	for i := 0; i < svcType.NumMethod(); i++ {
		method := svcType.Method(i)
		methodValue := svcValue.Method(i)
		methodName := method.Name

		switch {
		case method.Type.NumIn() == 3 &&
			method.Type.In(1).String() == "context.Context" &&
			hasPrefix(methodName, "RPC_"):
			stream := trimPrefix(methodName, "RPC_")
			s.frameworkCtx.Logger().Info("🔌 Registering RPC", map[string]interface{}{"method": methodName, "stream": stream})
			s.registerRPCMethod(stream, methodValue, method.Type)
		case method.Type.NumIn() == 2 && method.Type.In(0).String() == "context.Context" && hasPrefix(methodName, "Emitter_"):
			stream := trimPrefix(methodName, "Emitter_")
			s.registerEmitterPull(stream, 10*time.Second, methodValue)
		case method.Type.NumIn() == 2 && method.Type.In(0).String() == "context.Context" && hasPrefix(methodName, "Receiver_"):
			stream := trimPrefix(methodName, "Receiver_")
			s.registerReceiver(stream, 10, methodValue)
		}
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func trimPrefix(s, prefix string) string {
	if hasPrefix(s, prefix) {
		return s[len(prefix):]
	}
	return s
}

func (s *ServiceStruct) registerRPCMethod(stream string, method reflect.Value, methodType reflect.Type) {
	argType := methodType.In(1)
	retErrType := methodType.Out(1)
	if retErrType != reflect.TypeOf((*error)(nil)).Elem() {
		s.frameworkCtx.Logger().Error("Invalid RPC signature", map[string]interface{}{
			"method": method.String(),
		})
		return
	}

	handler := func(raw []byte) ([]byte, error) {
		start := time.Now()

		s.frameworkCtx.Metrics().Increment("rpc_request")
		defer s.frameworkCtx.Metrics().RecordTiming("rpc_request_time", time.Since(start))
		defer s.frameworkCtx.Compliance().TrackEvent("rpc_request")

		var msg transport.TransportMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			s.frameworkCtx.Logger().Error("Failed to decode TransportMessage", map[string]interface{}{"error": err.Error()})
			return nil, err
		}

		argPtr := reflect.New(argType)
		if err := json.Unmarshal(msg.Args, argPtr.Interface()); err != nil {
			s.frameworkCtx.Logger().Error("Failed to unmarshal args", map[string]interface{}{"error": err.Error()})
			return nil, err
		}

		if validator := s.frameworkCtx.Validator(); validator != nil {
			if err := validator.ValidateStruct(argPtr.Interface()); err != nil {
				s.frameworkCtx.Logger().Error("Validation failed", map[string]interface{}{"error": err.Error()})
				return nil, err
			}
		}

		ctx := s.frameworkCtx.Context()
		results := method.Call([]reflect.Value{reflect.ValueOf(ctx), argPtr.Elem()})
		if errVal := results[1]; !errVal.IsNil() {
			return nil, errVal.Interface().(error)
		}

		rawResponse, err := json.Marshal(results[0].Interface())
		if err != nil {
			return nil, fmt.Errorf("failed to encode response: %w", err)
		}

		if msg.ReplyTo != "" {
			reply := transport.NewTransportMessage(s.frameworkCtx.ServiceName(), stream, rawResponse)
			reply.MessageID = msg.MessageID
			reply.Trace = msg.Trace
			reply.Stash = msg.Stash

			replyBytes, err := json.Marshal(reply)
			if err != nil {
				s.frameworkCtx.Logger().Error("Failed to encode RPC reply", map[string]interface{}{"error": err.Error()})
				return nil, err
			}

			if err := s.frameworkCtx.Transport().Publish(msg.ReplyTo, replyBytes); err != nil {
				s.frameworkCtx.Logger().Error("Failed to publish RPC reply", map[string]interface{}{"error": err.Error()})
			}
		}

		return nil, nil
	}

	s.frameworkCtx.Supervisor().Go("rpc_"+stream, func(_ context.Context) error {
		if err := s.frameworkCtx.Transport().Subscribe(stream, handler); err != nil {
			s.frameworkCtx.Logger().Error("Failed to subscribe to RPC stream", map[string]interface{}{
				"stream": stream,
				"error":  err.Error(),
			})
			return err
		}
		return nil
	})
}

// registerReceiver registers a method to handle incoming messages.
func (s *ServiceStruct) registerReceiver(stream string, batchSize int, method reflect.Value) {
	batch := make([]*transport.TransportMessage, 0, batchSize)

	handler := func(raw []byte) ([]byte, error) {
		ctx := s.frameworkCtx.Context()

		msg := &transport.TransportMessage{}
		if err := json.Unmarshal(raw, msg); err != nil {
			s.frameworkCtx.Logger().Error("Receiver: failed to unmarshal message", map[string]interface{}{
				"stream": stream,
				"error":  err.Error(),
			})
			return nil, err
		}

		batch = append(batch, msg)

		if len(batch) < batchSize {
			return nil, nil
		}

		start := time.Now()
		args := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(batch)}

		results := method.Call(args)
		batch = batch[:0] // Clear the batch

		if len(results) > 0 && !results[len(results)-1].IsNil() {
			s.frameworkCtx.Logger().Error("Receiver error", map[string]interface{}{
				"stream": stream,
				"error":  results[len(results)-1].Interface().(error).Error(),
			})
			return nil, results[len(results)-1].Interface().(error)
		}

		s.frameworkCtx.Metrics().Increment("receiver_handled")
		s.frameworkCtx.Metrics().RecordTiming("receiver_handling_time", time.Since(start))
		s.frameworkCtx.Compliance().TrackEvent("receiver_message")

		return nil, nil
	}

	s.frameworkCtx.Supervisor().Go("receiver_"+stream, func(ctx context.Context) error {
		if err := s.frameworkCtx.Transport().Subscribe(stream, handler); err != nil {
			s.frameworkCtx.Logger().Error("Failed to subscribe to stream", map[string]interface{}{
				"stream": stream,
				"error":  err.Error(),
			})
			return err
		}
		return nil
	})
}

// registerEmitterPull registers a method to be called periodically.
func (s *ServiceStruct) registerEmitterPull(stream string, interval time.Duration, method reflect.Value) {
	s.frameworkCtx.Supervisor().Go("emitter_pull_"+stream, func(ctx context.Context) error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				start := time.Now()
				args := []reflect.Value{reflect.ValueOf(ctx)}
				results := method.Call(args)

				if len(results) > 0 && !results[len(results)-1].IsNil() {
					s.frameworkCtx.Logger().Error("Emitter pull error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					continue
				}

				if len(results) > 0 {
					if rawArgs, ok := results[0].Interface().(json.RawMessage); ok {
						tmsg := transport.NewTransportMessage(s.frameworkCtx.ServiceName(), stream, rawArgs)
						tmsg.Who = s.frameworkCtx.ServiceName()
						tmsg.Trace = map[string]string{"event": "emitter_pull"}

						encoded, err := tmsg.Encode()
						if err != nil {
							s.frameworkCtx.Logger().Error("Failed to marshal transport message", map[string]interface{}{
								"stream": stream,
								"error":  err.Error(),
							})
							continue
						}

						s.frameworkCtx.Metrics().Increment("emitter_pulled")
						s.frameworkCtx.Metrics().RecordTiming("emitter_pulling_time", time.Since(start))
						s.frameworkCtx.Compliance().TrackEvent("emitter_pull")

						if err := s.frameworkCtx.Transport().Publish(stream, encoded); err != nil {
							s.frameworkCtx.Logger().Error("Failed to publish message", map[string]interface{}{
								"stream": stream,
								"error":  err.Error(),
							})
						}
					} else {
						s.frameworkCtx.Logger().Error("Emitter pull returned non-json.RawMessage value", map[string]interface{}{
							"stream": stream,
						})
					}
				}
			}
		}
	})
}

// registerEmitterPoll registers a method to be called periodically and publishes all returned messages.
func (s *ServiceStruct) registerEmitterPoll(stream string, interval time.Duration, method reflect.Value) {
	s.frameworkCtx.Supervisor().Go("emitter_poll_"+stream, func(ctx context.Context) error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				start := time.Now()
				args := []reflect.Value{reflect.ValueOf(ctx)}
				results := method.Call(args)

				if len(results) > 0 && !results[len(results)-1].IsNil() {
					s.frameworkCtx.Logger().Error("Emitter poll error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					continue
				}

				if len(results) > 0 {
					items, ok := results[0].Interface().([][]byte)
					if !ok {
						s.frameworkCtx.Logger().Error("Emitter poll returned unexpected type", map[string]interface{}{
							"stream": stream,
						})
						continue
					}

					for _, raw := range items {
						msg := transport.NewTransportMessage(s.frameworkCtx.ServiceName(), stream, raw)
						msg.Who = s.frameworkCtx.ServiceName()
						msg.Trace = map[string]string{"source": "emitter_poll"}

						encoded, err := json.Marshal(msg)
						if err != nil {
							s.frameworkCtx.Logger().Error("Failed to encode message", map[string]interface{}{
								"error": err.Error(),
							})
							continue
						}

						s.frameworkCtx.Metrics().Increment("emitter_polled")
						s.frameworkCtx.Metrics().RecordTiming("emitter_polling_time", time.Since(start))
						s.frameworkCtx.Compliance().TrackEvent("emitter_poll")

						if err := s.frameworkCtx.Transport().Publish(stream, encoded); err != nil {
							s.frameworkCtx.Logger().Error("Failed to publish emitter message", map[string]interface{}{
								"stream": stream,
								"error":  err.Error(),
							})
						}
					}
				}
			}
		}
	})
}
