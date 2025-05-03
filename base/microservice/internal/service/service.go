package service

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/caaspay/caaspay-core/internal/transport"
	"github.com/caaspay/caaspay-core/pkg/api"
)

// ServiceStruct represents the core service structure with FrameworkContext.
type ServiceStruct struct {
	frameworkCtx    api.FrameworkContextInterface
	serviceInstance api.ServiceInterface
	shutdownCh      chan struct{}
	doneCh          chan struct{}
	wg              sync.WaitGroup
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
		shutdownCh:      make(chan struct{}),
		doneCh:          make(chan struct{}),
		lifecycle:       NewServiceLifecycle(),
	}, nil
}

// Run starts the service and its lifecycle.
func (s *ServiceStruct) Run() {
	defer close(s.doneCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.frameworkCtx.Logger().Info(ctx, "Starting service", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName(),
	})

	s.autoRegisterFunctions(s.serviceInstance)

	s.lifecycle.MarkStarted()

	if s.frameworkCtx.Config().Framework.HealthCheck.HTTPServerEnabled {
		s.healthServer = NewHealthServer(s)
		s.healthServer.Start()
	}

	if svc, ok := s.serviceInstance.(api.ServiceInterface); ok {
		if err := svc.Start(ctx); err != nil {
			s.frameworkCtx.Logger().Error(ctx, "❌ Service Start failed", map[string]interface{}{"error": err.Error()})
			return
		}

		if err := svc.HealthCheck(ctx); err != nil {
			s.frameworkCtx.Logger().Error(ctx, "❌ HealthCheck failed", map[string]interface{}{"error": err.Error()})
			return
		}
	}

	s.lifecycle.MarkReady()
	s.frameworkCtx.Logger().Info(ctx, "✅ Service marked as ready", nil)

	<-s.shutdownCh
	cancel()

	s.frameworkCtx.Logger().Info(ctx, "🛑 Shutting down service...", map[string]interface{}{
		"name": s.frameworkCtx.ServiceName(),
	})

	if svc, ok := s.serviceInstance.(api.ServiceInterface); ok {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer stopCancel()
		if err := svc.Stop(stopCtx); err != nil {
			s.frameworkCtx.Logger().Error(ctx, "❌ Service Stop failed", map[string]interface{}{"error": err.Error()})
		}
	}

	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		s.wg.Wait()
	}()

	select {
	case <-waitDone:
		s.frameworkCtx.Logger().Info(ctx, "✅ Shutdown complete", nil)
	case <-time.After(20 * time.Second):
		s.frameworkCtx.Logger().Error(ctx, "❌ Shutdown timed out", nil)
	}
}

// Shutdown gracefully signals the service to stop.
func (s *ServiceStruct) Shutdown() {
	if s.healthServer != nil {
		s.healthServer.Stop()
	}
	s.lifecycle.MarkShutdown()
	select {
	case <-s.shutdownCh:
	default:
		close(s.shutdownCh)
	}
}

func (s *ServiceStruct) Done() <-chan struct{} {
	return s.doneCh
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
			s.frameworkCtx.Logger().Info(context.Background(), "🔌 Registering RPC", map[string]interface{}{"method": methodName, "stream": stream})
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
		s.frameworkCtx.Logger().Error(context.Background(), "Invalid RPC signature", map[string]interface{}{"method": method.String()})
		return
	}

	handler := func(ctx context.Context, raw []byte) ([]byte, error) {
		start := time.Now()
		s.frameworkCtx.Metrics().Increment(ctx, "rpc_request")
		defer s.frameworkCtx.Metrics().RecordTiming(ctx, "rpc_request_time", time.Since(start))
		defer s.frameworkCtx.Compliance().TrackEvent(ctx, "rpc_request")

		var msg transport.TransportMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			s.frameworkCtx.Logger().Error(ctx, "Failed to decode TransportMessage", map[string]interface{}{"error": err.Error()})
			return nil, err
		}

		argPtr := reflect.New(argType)
		if err := json.Unmarshal(msg.Args, argPtr.Interface()); err != nil {
			s.frameworkCtx.Logger().Error(ctx, "Failed to unmarshal args", map[string]interface{}{"error": err.Error()})
			return nil, err
		}

		if validator := s.frameworkCtx.Validator(); validator != nil {
			if err := validator.ValidateStruct(argPtr.Interface()); err != nil {
				s.frameworkCtx.Logger().Error(ctx, "Validation failed", map[string]interface{}{"error": err.Error()})
				return nil, err
			}
		}

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
			if replyBytes, err := json.Marshal(reply); err == nil {
				if err := s.frameworkCtx.Transport().Publish(ctx, msg.ReplyTo, replyBytes); err != nil {
					s.frameworkCtx.Logger().Error(ctx, "Failed to publish RPC reply", map[string]interface{}{"error": err.Error()})
				}
			}
		}
		return nil, nil
	}

	s.frameworkCtx.Transport().Subscribe(stream, handler)
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
				s.frameworkCtx.Logger().Error(ctx, "Receiver: failed to unmarshal message", map[string]interface{}{
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
					s.frameworkCtx.Logger().Error(ctx, "Receiver error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					return nil, results[len(results)-1].Interface().(error)
				}

				s.frameworkCtx.Metrics().Increment(ctx, "receiver_handled")
				s.frameworkCtx.Metrics().RecordTiming(ctx, "receiver_handling_time", time.Since(start))
				s.frameworkCtx.Compliance().TrackEvent(ctx, "receiver_message")
			}
			return nil, nil
		}

		if err := s.frameworkCtx.Transport().Subscribe(stream, handler); err != nil {
			s.frameworkCtx.Logger().Error(context.Background(), "Failed to subscribe to stream", map[string]interface{}{
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
					s.frameworkCtx.Logger().Error(ctx, "Emitter pull error", map[string]interface{}{
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
							s.frameworkCtx.Logger().Error(ctx, "Failed to marshal transport message", map[string]interface{}{
								"stream": stream,
								"error":  err.Error(),
							})
							continue
						}

						s.frameworkCtx.Metrics().Increment(ctx, "emitter_pulled")
						s.frameworkCtx.Metrics().RecordTiming(ctx, "emitter_pulling_time", time.Since(start))
						s.frameworkCtx.Compliance().TrackEvent(ctx, "emitter_pull")

						if err := s.frameworkCtx.Transport().Publish(ctx, stream, encoded); err != nil {
							s.frameworkCtx.Logger().Error(ctx, "Failed to publish message", map[string]interface{}{
								"stream": stream,
								"error":  err.Error(),
							})
						}
					} else {
						s.frameworkCtx.Logger().Error(ctx, "Emitter pull returned non-json.RawMessage value", map[string]interface{}{
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
					s.frameworkCtx.Logger().Error(ctx, "Emitter poll error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					continue
				}

				if len(results) > 0 {
					items, ok := results[0].Interface().([][]byte)
					if !ok {
						s.frameworkCtx.Logger().Error(ctx, "Emitter poll returned unexpected type", nil)
						continue
					}

					for _, raw := range items {
						msg := transport.NewTransportMessage(s.frameworkCtx.ServiceName(), stream, raw)
						msg.Who = s.frameworkCtx.ServiceName()
						msg.Trace = map[string]string{"source": "emitter_poll"}

						encoded, err := json.Marshal(msg)
						if err != nil {
							s.frameworkCtx.Logger().Error(ctx, "Failed to encode message", map[string]interface{}{
								"error": err.Error(),
							})
							continue
						}

						s.frameworkCtx.Metrics().Increment(ctx, "emitter_polled")
						s.frameworkCtx.Metrics().RecordTiming(ctx, "emitter_polling_time", time.Since(start))
						s.frameworkCtx.Compliance().TrackEvent(ctx, "emitter_poll")

						if err := s.frameworkCtx.Transport().Publish(ctx, stream, encoded); err != nil {
							s.frameworkCtx.Logger().Error(ctx, "Failed to publish emitter message", map[string]interface{}{
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
