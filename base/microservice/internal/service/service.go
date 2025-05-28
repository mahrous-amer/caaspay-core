package service

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
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

	// Block until all supervised goroutines finish
	// <-s.supervisor.Done()
	// waiting on supervisor here will cause a deadlock.
	// Block until shutdown requested via fwCtx.Context
	<-s.frameworkCtx.Context().Done()

	s.frameworkCtx.Logger().Info("🛑 Service.Run exiting due to context cancellation", nil)
}

// Shutdown gracefully signals the service to stop.
func (s *ServiceStruct) Shutdown() {
	// call service stop first
	if err := s.serviceInstance.Stop(); err != nil {
		s.frameworkCtx.Logger().Error("❌ Service Stop failed", map[string]interface{}{"error": err.Error()})
	}

	s.frameworkCtx.Logger().Info("Service shutting down", nil)
	if s.healthServer != nil {
		s.healthServer.Stop()
	}

	s.lifecycle.MarkShutdown()
	s.frameworkCtx.Transport().CleanupOnShutdown()
	s.supervisor.Shutdown()

	// Wait for all supervised goroutines to finish
	//<-s.supervisor.Done()
	//s.supervisor.StopAll()

}

func (s *ServiceStruct) Done() <-chan struct{} {
	return s.supervisor.Done()
}

func (s *ServiceStruct) autoRegisterFunctions(serviceInstance interface{}) {
	svcValue := reflect.ValueOf(serviceInstance)
	svcType := reflect.TypeOf(serviceInstance)

	s.frameworkCtx.Logger().Info("🔌 AUTO Registering", map[string]interface{}{
		"svcType": svcType.String(),
	})

	for i := 0; i < svcType.NumMethod(); i++ {
		method := svcType.Method(i)
		methodValue := svcValue.Method(i)
		methodType := method.Type
		methodName := method.Name

		switch {
		// 🚀 RPC METHOD
		// Example: func RPC_Ping(req PingRequest) (PingResponse, error)
		case strings.HasPrefix(methodName, "RPC_"):
			stream := transport.BuildStreamName(api.StreamConfig{
				Type:    api.StreamTypeRPC,
				Service: s.frameworkCtx.ServiceName(),
				Method:  trimPrefix(methodName, "RPC_"),
			})
			s.frameworkCtx.Logger().Info("🔌 Registering RPC", map[string]interface{}{
				"method": methodName, "stream": stream,
			})
			s.registerRPCMethod(stream, methodValue, methodType)

		// 🔁 EmitterPull
		// Example: func EmitterPull_Heartbeat(ctx context.Context) ([]*TransportMessage, time.Duration, error)
		case strings.HasPrefix(methodName, "EmitterPull_"):
			stream := transport.BuildStreamName(api.StreamConfig{
				Type:    api.StreamTypeEmitter,
				Service: s.frameworkCtx.ServiceName(),
				Method:  trimPrefix(methodName, "EmitterPull_"),
			})
			s.frameworkCtx.Logger().Info("🔌 Registering EmitterPull", map[string]interface{}{
				"method": methodName, "stream": stream,
			})
			s.registerEmitterPull(stream, methodValue)

		// 📡 EmitterChannel
		// Example: func EmitterChannel_PushLog(ctx context.Context, ch chan *TransportMessage)
		case strings.HasPrefix(methodName, "EmitterChannel_"):
			stream := transport.BuildStreamName(api.StreamConfig{
				Type:    api.StreamTypeEmitter,
				Service: s.frameworkCtx.ServiceName(),
				Method:  trimPrefix(methodName, "EmitterChannel_"),
			})
			s.frameworkCtx.Logger().Info("🔌 Registering EmitterChannel", map[string]interface{}{
				"method": methodName, "stream": stream,
			})
			s.registerEmitterChannel(stream, methodValue)

		// 📥 Receiver
		// Example: func Receiver_other__service_Heartbeat(ctx context.Context, msg *HeartbeatMessage) error
		case strings.HasPrefix(methodName, "Receiver_"):
			methodName = strings.ReplaceAll(methodName, "__", "-")
			parts := strings.SplitN(trimPrefix(methodName, "Receiver_"), "_", 2)

			if len(parts) != 2 {
				s.frameworkCtx.Logger().Error("❌ Invalid Receiver method name format. Expected: Receiver_Service_Method", map[string]interface{}{
					"method": methodName,
				})
				continue
			}

			if methodType.NumIn() != 3 || methodType.NumOut() != 1 {
				s.frameworkCtx.Logger().Error("❌ Receiver must have signature: func(context.Context, *Struct) error", map[string]interface{}{
					"method": methodName,
				})
				continue
			}

			argType := methodType.In(2)
			if argType.Kind() != reflect.Ptr || argType.Elem().Kind() != reflect.Struct {
				s.frameworkCtx.Logger().Error("❌ Receiver 2nd argument must be pointer to a struct", map[string]interface{}{
					"method": methodName,
				})
				continue
			}

			errType := methodType.Out(0)
			if errType != reflect.TypeOf((*error)(nil)).Elem() {
				s.frameworkCtx.Logger().Error("❌ Receiver return type must be error", map[string]interface{}{
					"method": methodName,
				})
				continue
			}

			sourceService := strings.ToLower(parts[0])
			sourceMethod := strings.ToLower(parts[1])
			stream := transport.BuildStreamName(api.StreamConfig{
				Type:    api.StreamTypeEmitter,
				Service: sourceService,
				Method:  sourceMethod,
			})

			s.frameworkCtx.Logger().Info("🔌 Registering Receiver", map[string]interface{}{
				"method": methodName, "stream": stream,
			})

			s.registerReceiver(stream, argType, methodValue)
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
	if methodType.NumIn() != 2 || methodType.NumOut() != 2 {
		s.frameworkCtx.Logger().Error("Invalid RPC method signature", map[string]interface{}{
			"method": method.String(),
			"note":   "expected: func(InputType) (OutputType, error)",
		})
		return
	}

	argType := methodType.In(1)
	errType := methodType.Out(1)
	if errType != reflect.TypeOf((*error)(nil)).Elem() {
		s.frameworkCtx.Logger().Error("Invalid RPC return type", map[string]interface{}{
			"method": method.String(),
		})
		return
	}

	handler := func(raw []byte) ([]byte, error) {
		metrics := s.frameworkCtx.Metrics()
		logger := s.frameworkCtx.Logger()
		compliance := s.frameworkCtx.Compliance()

		timing := struct {
			start        time.Time
			decode       time.Duration
			validate     time.Duration
			execute      time.Duration
			encode       time.Duration
			replyEncode  time.Duration
			replyPublish time.Duration
			total        time.Duration
		}{start: time.Now()}

		metrics.IncrementTagged("service_requests", "stream", stream)
		compliance.TrackEvent("rpc_request")

		var msg api.TransportMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			logger.Error("Failed to decode TransportMessage", map[string]interface{}{
				"error":  err.Error(),
				"stream": stream,
			})
			metrics.IncrementTagged("request_errors", "stream", stream, "stage", "decode")
			return nil, err
		}
		timing.decode = time.Since(timing.start)

		start := time.Now()
		argPtr := reflect.New(argType)
		if err := json.Unmarshal(msg.Args, argPtr.Interface()); err != nil {
			logger.Error("Failed to unmarshal args", map[string]interface{}{
				"error":  err.Error(),
				"stream": stream,
			})
			metrics.IncrementTagged("request_errors", "stream", stream, "stage", "args_unmarshal")
			return nil, err
		}

		if validator := s.frameworkCtx.Validator(); validator != nil {
			if err := validator.ValidateStruct(argPtr.Interface()); err != nil {
				logger.Error("Validation failed", map[string]interface{}{
					"error":  err.Error(),
					"stream": stream,
				})
				metrics.IncrementTagged("request_errors", "stream", stream, "stage", "validation")
				return nil, err
			}
		}
		timing.validate = time.Since(start)

		start = time.Now()
		results := method.Call([]reflect.Value{argPtr.Elem()})
		timing.execute = time.Since(start)

		if errVal := results[1]; !errVal.IsNil() {
			metrics.IncrementTagged("request_errors", "stream", stream, "stage", "handler")
			return nil, errVal.Interface().(error)
		}

		start = time.Now()
		rawResponse, err := json.Marshal(results[0].Interface())
		if err != nil {
			metrics.IncrementTagged("request_errors", "stream", stream, "stage", "response_marshal")
			return nil, fmt.Errorf("failed to encode response: %w", err)
		}
		timing.encode = time.Since(start)

		if msg.ReplyTo != "" {
			start = time.Now()
			reply := api.NewTransportMessage(s.frameworkCtx.ServiceName(), stream, nil)
			reply.MessageID = msg.MessageID
			reply.Trace = msg.Trace
			reply.Stash = msg.Stash
			reply.Response = rawResponse

			replyBytes, err := json.Marshal(reply)
			timing.replyEncode = time.Since(start)
			if err != nil {
				logger.Error("Failed to encode RPC reply", map[string]interface{}{
					"error":  err.Error(),
					"stream": stream,
				})
				return nil, err
			}

			start = time.Now()
			if err := s.frameworkCtx.Transport().Publish(s.frameworkCtx.Context(), msg.ReplyTo, replyBytes); err != nil {
				logger.Error("Failed to publish RPC reply", map[string]interface{}{
					"error":  err.Error(),
					"stream": stream,
				})
				metrics.IncrementTagged("request_errors", "stream", stream, "stage", "publish")
			}
			timing.replyPublish = time.Since(start)
		}

		timing.total = time.Since(timing.start)
		metrics.IncrementTagged("service_requests", "stream", stream)

		metrics.ObserveHistogram("framework.rpc.total", timing.total.Seconds(), "stream", stream)
		metrics.ObserveHistogram("framework.rpc.decode", timing.decode.Seconds(), "stream", stream)
		metrics.ObserveHistogram("framework.rpc.validate", timing.validate.Seconds(), "stream", stream)
		metrics.ObserveHistogram("framework.rpc.execute", timing.execute.Seconds(), "stream", stream)
		metrics.ObserveHistogram("framework.rpc.encode", timing.encode.Seconds(), "stream", stream)
		metrics.ObserveHistogram("framework.rpc.reply_encode", timing.replyEncode.Seconds(), "stream", stream)
		metrics.ObserveHistogram("framework.rpc.reply_publish", timing.replyPublish.Seconds(), "stream", stream)

		logger.Info("⏱️ RPC Timings", map[string]interface{}{
			"stream":        stream,
			"decode_ms":     timing.decode.Milliseconds(),
			"validate_ms":   timing.validate.Milliseconds(),
			"execute_ms":    timing.execute.Milliseconds(),
			"encode_ms":     timing.encode.Milliseconds(),
			"reply_encode":  timing.replyEncode.Milliseconds(),
			"reply_publish": timing.replyPublish.Milliseconds(),
			"total_ms":      timing.total.Milliseconds(),
		})

		return nil, nil
	}

	s.frameworkCtx.Supervisor().Go("subscribe_"+stream, func(_ context.Context) error {
		if err := s.frameworkCtx.Transport().Subscribe("rpc_cg", stream, handler); err != nil {
			s.frameworkCtx.Logger().Error("Failed to subscribe to RPC stream", map[string]interface{}{
				"stream": stream,
				"error":  err.Error(),
			})
			return err
		}
		return nil
	})
}

func (s *ServiceStruct) registerEmitterPull(stream string, method reflect.Value) {
	s.frameworkCtx.Supervisor().GoLoop("emitter_pull_"+stream, func(ctx context.Context) (time.Duration, error) {
		start := time.Now()

		// ✅ Safety timeout wrapper to prevent hangs
		safeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		select {
		case <-safeCtx.Done():
			s.frameworkCtx.Logger().Info("EmitterPull skipped due to cancellation", map[string]interface{}{
				"stream": stream,
			})
			return 0, safeCtx.Err()
		default:
		}

		// 👇 Proceed with reflection-based call
		results := method.Call([]reflect.Value{reflect.ValueOf(safeCtx)})

		if len(results) != 3 {
			s.frameworkCtx.Logger().Error("EmitterPull returned unexpected result count", map[string]interface{}{
				"stream": stream,
			})
			s.frameworkCtx.Metrics().IncrementTagged("emitter_pull_errors", "stream", stream, "stage", "signature")
			return 5 * time.Second, nil
		}

		messages := results[0].Interface().([]*api.TransportMessage)
		interval := results[1].Interface().(time.Duration)
		var err error
		if !results[2].IsNil() {
			err = results[2].Interface().(error)
		}

		for _, msg := range messages {
			if msg == nil {
				continue
			}
			encoded, err := msg.Encode()
			if err != nil {
				s.frameworkCtx.Logger().Error("EmitterPull encode failed", map[string]interface{}{
					"stream": stream, "error": err.Error(),
				})
				s.frameworkCtx.Metrics().IncrementTagged("emitter_pull_errors", "stream", stream, "stage", "encode")
				continue
			}
			select {
			case <-safeCtx.Done():
				s.frameworkCtx.Logger().Info("EmitterPull skipped due to cancellation", map[string]interface{}{
					"stream": stream,
				})
				return 0, safeCtx.Err()
			default:
			}
			if err := s.frameworkCtx.Transport().Publish(safeCtx, stream, encoded); err != nil {
				s.frameworkCtx.Logger().Error("EmitterPull publish failed", map[string]interface{}{
					"stream": stream, "error": err.Error(),
				})
				s.frameworkCtx.Metrics().IncrementTagged("emitter_pull_errors", "stream", stream, "stage", "publish")
				continue
			}

			s.frameworkCtx.Logger().Debug("EmitterPull emitted message", map[string]interface{}{
				"stream": stream,
			})
			s.frameworkCtx.Compliance().TrackEvent("emitter_pull_emit")
			s.frameworkCtx.Metrics().IncrementTagged("emitter_pull_emitted", "stream", stream)
		}

		s.frameworkCtx.Metrics().ObserveHistogram("emitter_pull_duration", time.Since(start).Seconds(), "stream", stream)
		return interval, err
	})
}

func (s *ServiceStruct) registerEmitterChannel(stream string, method reflect.Value) {
	ch := make(chan *api.TransportMessage, 100)
	// 👇 Inject the channel once for service usage
	method.Call([]reflect.Value{
		reflect.ValueOf(s.frameworkCtx.Context()),
		reflect.ValueOf(ch),
	})

	// 🎯 Framework emitter that reads from the channel and emits messages
	s.frameworkCtx.Supervisor().GoLoop("emitter_channel_"+stream, func(ctx context.Context) (time.Duration, error) {
		select {
		case <-ctx.Done():
			return 1 * time.Second, nil

		case msg := <-ch:
			if msg == nil {
				return 100 * time.Millisecond, nil
			}

			start := time.Now()

			encoded, err := msg.Encode()
			if err != nil {
				s.frameworkCtx.Logger().Error("Channel emitter encode error", map[string]interface{}{
					"stream": stream, "error": err.Error(),
				})
				s.frameworkCtx.Metrics().IncrementTagged("emitter_channel_errors", "stream", stream, "stage", "encode")
				return 100 * time.Millisecond, nil
			}

			if err := s.frameworkCtx.Transport().Publish(ctx, stream, encoded); err != nil {
				s.frameworkCtx.Logger().Error("Channel emitter publish error", map[string]interface{}{
					"stream": stream, "error": err.Error(),
				})
				s.frameworkCtx.Metrics().IncrementTagged("emitter_channel_errors", "stream", stream, "stage", "publish")
				return 100 * time.Millisecond, nil
			}

			s.frameworkCtx.Logger().Debug("Channel emitted message", map[string]interface{}{
				"stream": stream,
			})
			s.frameworkCtx.Compliance().TrackEvent("emitter_channel_emit")
			s.frameworkCtx.Metrics().IncrementTagged("emitter_channel_emitted", "stream", stream)
			s.frameworkCtx.Metrics().ObserveHistogram("emitter_channel_duration", time.Since(start).Seconds(), "stream", stream)

			return 100 * time.Millisecond, nil
		}
	})

}

func (s *ServiceStruct) registerReceiver(stream string, argType reflect.Type, method reflect.Value) {
	if argType.Kind() != reflect.Ptr || argType.Elem().Kind() != reflect.Struct {
		s.frameworkCtx.Logger().Error("❌ Receiver argument must be pointer to a struct", map[string]interface{}{
			"stream": stream,
		})
		return
	}

	handler := func(raw []byte) ([]byte, error) {
		ctx := s.frameworkCtx.Context()

		var msg api.TransportMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			s.frameworkCtx.Logger().Error("Receiver: failed to decode TransportMessage", map[string]interface{}{
				"stream": stream, "error": err.Error(),
			})
			s.frameworkCtx.Metrics().IncrementTagged("receiver_errors", "stream", stream, "stage", "unmarshal_transport")
			return nil, err
		}

		argPtr := reflect.New(argType.Elem())
		if err := json.Unmarshal(msg.Args, argPtr.Interface()); err != nil {
			s.frameworkCtx.Logger().Error("Receiver: failed to unmarshal message body", map[string]interface{}{
				"stream": stream, "error": err.Error(),
			})
			s.frameworkCtx.Metrics().IncrementTagged("receiver_errors", "stream", stream, "stage", "unmarshal_payload")
			return nil, err
		}

		if validator := s.frameworkCtx.Validator(); validator != nil {
			if err := validator.ValidateStruct(argPtr.Interface()); err != nil {
				s.frameworkCtx.Logger().Error("Receiver: validation failed", map[string]interface{}{
					"stream": stream, "error": err.Error(),
				})
				s.frameworkCtx.Metrics().IncrementTagged("receiver_errors", "stream", stream, "stage", "validation")
				return nil, err
			}
		}

		start := time.Now()
		results := method.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			argPtr,
		})
		if errVal := results[0]; !errVal.IsNil() {
			err := errVal.Interface().(error)
			s.frameworkCtx.Logger().Error("Receiver handler returned error", map[string]interface{}{
				"stream": stream, "error": err.Error(),
			})
			s.frameworkCtx.Metrics().IncrementTagged("receiver_errors", "stream", stream, "stage", "handler")
			return nil, err
		}

		s.frameworkCtx.Logger().Debug("✅ Receiver handled message", map[string]interface{}{
			"stream": stream,
		})
		s.frameworkCtx.Metrics().IncrementTagged("receiver_handled", "stream", stream)
		s.frameworkCtx.Metrics().ObserveHistogram("receiver_duration", time.Since(start).Seconds(), "stream", stream)
		s.frameworkCtx.Compliance().TrackEvent("receiver_message_processed")
		return nil, nil
	}

	s.frameworkCtx.Supervisor().Go("receiver_CG_"+stream, func(ctx context.Context) error {
		if err := s.frameworkCtx.Transport().Subscribe("receiver_cg", stream, handler); err != nil {
			s.frameworkCtx.Logger().Error("Receiver failed to subscribe", map[string]interface{}{
				"stream": stream, "error": err.Error(),
			})
			return err
		}
		return nil
	})
}
