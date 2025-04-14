package service

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/caaspay/caaspay-core/internal/logging"
	"github.com/caaspay/caaspay-core/internal/metrics"
	"github.com/caaspay/caaspay-core/internal/transport"
	"github.com/caaspay/caaspay-core/internal/config"
	"github.com/caaspay/caaspay-core/internal/compliance"
)

// Service defines the interface for our core service
type Service interface {
	// Start initializes the service
	Start(ctx context.Context) error
	// Stop gracefully shuts down the service
	Stop(ctx context.Context) error
	// HealthCheck returns the health status of the service
	HealthCheck(ctx context.Context) error
}

// NewService creates a new instance of the service
func NewService() Service {
	return &serviceImpl{}
}

type serviceImpl struct {
	// Add service-specific fields here
}

func (s *serviceImpl) Start(ctx context.Context) error {
	// TODO: Implement service startup logic
	return nil
}

func (s *serviceImpl) Stop(ctx context.Context) error {
	// TODO: Implement service shutdown logic
	return nil
}

func (s *serviceImpl) HealthCheck(ctx context.Context) error {
	// TODO: Implement health check logic
	return nil
}

// ServiceStruct represents the core service structure.
type ServiceStruct struct {
	cfg             *config.Config
	logger          *logging.Logger
	metrics         *metrics.Metrics
	compliance      *compliance.ComplianceReporter
	transport       transport.Transport
	serviceInstance interface{}
	shutdownCh      chan struct{}
	wg              sync.WaitGroup
}

// NewServiceStruct creates a new service instance.
func NewServiceStruct(cfg *config.Config, serviceInstance interface{}) (*ServiceStruct, error) {
	// Initialize logger
	logger := logging.NewLogger(cfg.Framework.ServiceName, cfg.Framework.Logging.Level, cfg.Framework.Logging.RedactSensitive)

	// Initialize metrics
	metricsInstance, err := metrics.NewMetrics(cfg.Framework.ServiceName, &cfg.Framework.Observability)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	// Initialize transport
	redisCfg := transport.RedisTransportConfig{
		RedisAddr:          cfg.Framework.Transport.RedisAddress,
		UseCompression:     cfg.Framework.Transport.UseCompression,
		UseEncryption:      cfg.Framework.Transport.UseEncryption,
		ServiceReplyStream: fmt.Sprintf("%s_reply", cfg.Framework.ServiceName),
		MaxRetries:         3,
		RetryDelay:         500 * time.Millisecond,
	}
	redisTransport := transport.NewRedisTransport(redisCfg, logger)

	// Initialize compliance reporter
	complianceReporter := compliance.NewComplianceReporter(cfg, metricsInstance)

	service := &ServiceStruct{
		cfg:             cfg,
		logger:          logger,
		metrics:         metricsInstance,
		compliance:      complianceReporter,
		transport:       redisTransport,
		serviceInstance: serviceInstance,
		shutdownCh:      make(chan struct{}),
	}

	// Register RPC methods
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

		// Get method type (excluding the receiver)
		methodType := method.Type
		if methodType.NumIn() < 3 || methodType.NumOut() < 1 {
			continue
		}

		// Check if method has RPC signature (context.Context, []byte)
		if methodType.In(1).String() == "context.Context" && methodType.In(2).String() == "[]byte" {
			stream := method.Name
			if err := s.registerRPCMethod(stream, map[string]string{"stream": stream}); err != nil {
				return fmt.Errorf("failed to register RPC method %s: %w", method.Name, err)
			}
		}
	}
	return nil
}

// registerRPCMethod registers a single RPC method.
func (s *ServiceStruct) registerRPCMethod(methodName string, config map[string]string) error {
	// Get the method from the service instance
	method := reflect.ValueOf(s.serviceInstance).MethodByName(methodName)
	if !method.IsValid() {
		return fmt.Errorf("method %s not found", methodName)
	}

	// Register the method with the transport
	stream := config["stream"]
	if stream == "" {
		return fmt.Errorf("stream not specified for method %s", methodName)
	}

	if err := s.transport.Subscribe(stream, func(ctx context.Context, data []byte) ([]byte, error) {
		// Call the method with the context and data
		args := []reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(data),
		}
		results := method.Call(args)

		// Check for errors
		if len(results) > 0 && !results[len(results)-1].IsNil() {
			return nil, results[len(results)-1].Interface().(error)
		}

		// Return the result
		if len(results) > 0 {
			return results[0].Interface().([]byte), nil
		}
		return nil, nil
	}); err != nil {
		return fmt.Errorf("failed to subscribe to stream %s: %w", stream, err)
	}

	return nil
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
				results := method.Call(args)

				if len(results) > 0 && !results[len(results)-1].IsNil() {
					s.logger.Error(ctx, "Emitter pull error", map[string]interface{}{
						"stream": stream,
						"error":  results[len(results)-1].Interface().(error).Error(),
					})
					continue
				}

				if len(results) > 0 {
					if data, ok := results[0].Interface().([]byte); ok {
						if err := s.transport.Publish(ctx, stream, data); err != nil {
							s.logger.Error(ctx, "Failed to publish message", map[string]interface{}{
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
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		if err := s.transport.Subscribe(stream, func(ctx context.Context, data []byte) ([]byte, error) {
			batch = append(batch, data)
			if len(batch) >= batchSize {
				args := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(batch)}
				results := method.Call(args)
				batch = batch[:0]

				if len(results) > 0 && !results[len(results)-1].IsNil() {
					return nil, results[len(results)-1].Interface().(error)
				}
			}
			return nil, nil
		}); err != nil {
			s.logger.Error(context.Background(), "Failed to subscribe to stream", map[string]interface{}{
				"stream": stream,
				"error":  err.Error(),
			})
		}
	}()
}

// AutoRegisterFunctions scans the struct for eligible framework methods and registers them.
func (s *ServiceStruct) AutoRegisterFunctions(serviceInstance interface{}) {
	svcType := reflect.TypeOf(serviceInstance)
	svcValue := reflect.ValueOf(serviceInstance)

	for i := 0; i < svcType.NumMethod(); i++ {
		method := svcType.Method(i)
		methodValue := svcValue.Method(i)
		streamName := method.Name
		timeout := 30 * time.Second
		batchSize := 1
		interval := 10 * time.Second

		// Check method signature for RPC
		if method.Type.NumIn() == 3 && method.Type.In(1).String() == "context.Context" && method.Type.In(2).String() == "[]byte" {
			s.registerRPCMethod(streamName, map[string]string{"stream": streamName, "timeout": fmt.Sprintf("%d", int(timeout))})
		}

		// Check method signature for emitter pull
		if method.Type.NumIn() == 2 && method.Type.In(1).String() == "context.Context" {
			s.registerEmitterPull(streamName, interval, methodValue)
		}

		// Check method signature for receiver
		if method.Type.NumIn() == 3 && method.Type.In(1).String() == "context.Context" && method.Type.In(2).String() == "[][]byte" {
			s.registerReceiver(streamName, batchSize, methodValue)
		}
	}
}

// Run starts the service lifecycle
func (s *ServiceStruct) Run(serviceInstance interface{}) {
	ctx := context.Background()
	s.logger.Info(ctx, "Starting service", map[string]interface{}{
		"name": s.cfg.Framework.ServiceName,
	})
	s.AutoRegisterFunctions(serviceInstance)
	<-s.shutdownCh
	s.logger.Info(ctx, "Shutting down service", map[string]interface{}{
		"name": s.cfg.Framework.ServiceName,
	})
	s.wg.Wait()
}

// Shutdown gracefully stops the service
func (s *ServiceStruct) Shutdown() {
	ctx := context.Background()
	s.logger.Info(ctx, "Shutting down service", map[string]interface{}{
		"name": s.cfg.Framework.ServiceName,
	})
	close(s.shutdownCh)
}

// Request performs an RPC call using the transport layer.
func (s *ServiceStruct) Request(ctx context.Context, stream string, data []byte, timeout time.Duration) ([]byte, error) {
	s.metrics.Increment(ctx, "rpc_request")
	return s.transport.Request(ctx, stream, data, timeout)
}

// Publish sends a message to a stream.
func (s *ServiceStruct) Publish(ctx context.Context, stream string, data []byte) error {
	s.metrics.Increment(ctx, "event_published")
	return s.transport.Publish(ctx, stream, data)
}

// Subscribe registers a handler to process incoming messages.
func (s *ServiceStruct) Subscribe(stream string, handler transport.HandlerFunc) error {
	s.metrics.Increment(context.Background(), "subscriber_registered")
	return s.transport.Subscribe(stream, handler)
}

// EmitterPolling sends messages via a managed channel.
func (s *ServiceStruct) EmitterPolling(stream string) chan []byte {
	ch := make(chan []byte, 100)

	go func() {
		for {
			msg, ok := <-ch
			if !ok {
				s.logger.Info(context.Background(), "Channel closed, stopping emitter polling", map[string]interface{}{
					"stream": stream,
				})
				break
			}
			s.metrics.Increment(context.Background(), "emitter_polling")
			s.compliance.TrackEvent(context.Background(), "emitter_polling")
			if err := s.transport.Publish(context.Background(), stream, msg); err != nil {
				s.logger.Error(context.Background(), "Failed to publish message", map[string]interface{}{
					"stream": stream,
					"error":  err.Error(),
				})
			} else {
				s.logger.Info(context.Background(), "Message emitted", map[string]interface{}{
					"stream": stream,
				})
			}
		}
	}()

	return ch
}

// EmitterPulling continuously executes a function and emits its output.
func (s *ServiceStruct) EmitterPulling(stream string, fetch func() ([]byte, error), interval time.Duration) {
	go func() {
		for {
			start := time.Now()
			msg, err := fetch()
			if err != nil {
				s.logger.Error(context.Background(), "EmitterPulling error", map[string]interface{}{
					"stream": stream,
					"error":  err.Error(),
				})
			} else {
				s.metrics.Increment(context.Background(), "emitter_pulling")
				s.metrics.RecordTiming(context.Background(), "emitter_pulling_time", time.Since(start))
				s.compliance.TrackEvent(context.Background(), "emitter_pulling")
				if err := s.transport.Publish(context.Background(), stream, msg); err != nil {
					s.logger.Error(context.Background(), "Failed to publish pulled message", map[string]interface{}{
						"stream": stream,
						"error":  err.Error(),
					})
				}
			}
			time.Sleep(interval)
		}
	}()
}

// Receiver subscribes to a transport stream and processes incoming messages.
func (s *ServiceStruct) Receiver(stream string, handler func(ctx context.Context, data []byte)) {
	go func() {
		if err := s.transport.Subscribe(stream, func(ctx context.Context, msg []byte) ([]byte, error) {
			s.metrics.Increment(ctx, "receiver_messages")
			s.compliance.TrackEvent(ctx, "receiver_messages")
			handler(ctx, msg)
			return nil, nil
		}); err != nil {
			s.logger.Error(context.Background(), "Failed to subscribe to stream", map[string]interface{}{
				"stream": stream,
				"error":  err.Error(),
			})
		}
	}()
}
