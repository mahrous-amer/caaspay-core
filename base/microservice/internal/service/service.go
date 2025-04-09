package service

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"

	"caaspay-core/internal/logging"
	"caaspay-core/internal/metrics"
	"caaspay-core/internal/transport"
	"caaspay-core/internal/config"
	"caaspay-core/internal/compliance"
)

// Service manages the lifecycle and behavior of a microservice.
type Service struct {
	Name               string
	Logger             *logging.Logger
	Metrics            *metrics.Metrics
	Transport          transport.Transport
	API                transport.API
	ChannelMgr         *ChannelManager
	ComplianceReporter *compliance.ComplianceReporter
	Config             *config.Config
	shutdownCh         chan struct{}
	wg                 sync.WaitGroup
}

// NewService initializes a service with logging, metrics, compliance tracking, and transport.
func NewService(configPath string, serviceConfigPath string) *Service {
	fmt.Println("🚀 Initializing Service Framework")

	cfg, err := config.LoadConfig(configPath, serviceConfigPath)
	if err != nil {
		log.Fatalf("❌ Failed to load configuration: %v", err)
	}

	logger := logging.NewLogger(cfg.Framework.ServiceName)

	transportCfg := transport.RedisTransportConfig{
		RedisAddr:          cfg.Framework.Transport.RedisAddress,
		UseCompression:     cfg.Framework.Transport.UseCompression,
		UseEncryption:      cfg.Framework.Transport.UseEncryption,
		ServiceReplyStream: fmt.Sprintf("%s_reply", cfg.Framework.ServiceName),
	}
	redisTransport := transport.NewRedisTransport(transportCfg)

	metricsInstance, err := metrics.NewMetrics(cfg.Framework.ServiceName, fmt.Sprintf("%s:%d", cfg.Framework.Observability.MetricsHost, cfg.Framework.Observability.MetricsPort))
	if err != nil {
		log.Fatalf("❌ Failed to initialize metrics: %v", err)
	}

	complianceReporter := compliance.NewComplianceReporter(metricsInstance, logger, cfg)

	return &Service{
		Name:               cfg.Framework.ServiceName,
		Logger:             logger,
		Metrics:            metricsInstance,
		Transport:          redisTransport,
		API:                redisTransport,
		ChannelMgr:         NewChannelManager(),
		ComplianceReporter: complianceReporter,
		shutdownCh:         make(chan struct{}),
	}
}

// AutoRegisterFunctions scans the struct for eligible framework methods and registers them.
func (s *Service) AutoRegisterFunctions(serviceInstance interface{}) {
	svcType := reflect.TypeOf(serviceInstance)
	svcValue := reflect.ValueOf(serviceInstance)

	for i := 0; i < svcType.NumMethod(); i++ {
		method := svcType.Method(i)
		methodValue := svcValue.Method(i)
		streamName := method.Name
		timeout := 30 * time.Second
		batchSize := 1
		interval := 10 * time.Second

		if tag, ok := method.Tag.Lookup("rpc"); ok {
			if tag != "" {
				streamName = tag
			}
			if timeoutTag, ok := method.Tag.Lookup("timeout"); ok {
				t, err := strconv.Atoi(timeoutTag)
				if err == nil {
					timeout = time.Duration(t) * time.Second
				}
			}
			s.registerRPCMethod(streamName, timeout, methodValue)
		} else if tag, ok := method.Tag.Lookup("emitter_pull"); ok {
			if tag != "" {
				streamName = tag
			}
			if intervalTag, ok := method.Tag.Lookup("interval"); ok {
				t, err := strconv.Atoi(intervalTag)
				if err == nil {
					interval = time.Duration(t) * time.Second
				}
			}
			s.registerEmitterPull(streamName, interval, methodValue)
		} else if tag, ok := method.Tag.Lookup("receiver"); ok {
			if tag != "" {
				streamName = tag
			}
			if batchTag, ok := method.Tag.Lookup("batch_size"); ok {
				b, err := strconv.Atoi(batchTag)
				if err == nil {
					batchSize = b
				}
			}
			s.registerReceiver(streamName, batchSize, methodValue)
		}
	}
}

// Run starts the service lifecycle
func (s *Service) Run(serviceInstance interface{}) {
	s.Logger.Info("Starting service", "name", s.Name)
	s.AutoRegisterFunctions(serviceInstance)
	<-s.shutdownCh
	s.Logger.Info("Shutting down service", "name", s.Name)
	s.wg.Wait()
}

// Shutdown gracefully stops the service
func (s *Service) Shutdown() {
	s.Logger.Info("Shutting down service", "name", s.Name)
	close(s.shutdownCh)
}

// Request performs an RPC call using the transport layer.
func (s *Service) Request(ctx context.Context, stream string, data []byte, timeout time.Duration) ([]byte, error) {
	s.Metrics.Increment("rpc_request")
	return s.Transport.Request(ctx, stream, data, timeout)
}

// Publish sends a message to a stream.
func (s *Service) Publish(ctx context.Context, stream string, data []byte) error {
	s.Metrics.Increment("event_published")
	return s.Transport.Publish(ctx, stream, data)
}

// Subscribe registers a handler to process incoming messages.
func (s *Service) Subscribe(stream string, handler transport.HandlerFunc) error {
	s.Metrics.Increment("subscriber_registered")
	return s.Transport.Subscribe(stream, handler)
}

// EmitterPolling sends messages via a managed channel.
func (s *Service) EmitterPolling(stream string) chan []byte {
	ch := s.ChannelMgr.CreateChannel(stream, 100)

	go func() {
		for {
			msg, ok := <-ch
			if !ok {
				s.Logger.Info("🛑 Channel closed, stopping emitter polling", "stream", stream)
				break
			}
			s.Metrics.Increment("emitter_polling")
			s.ComplianceReporter.TrackEvent("emitter_polling")
			if err := s.API.Publish(stream, msg); err != nil {
				s.Logger.Error("❌ Failed to publish message", "stream", stream, "error", err)
			} else {
				s.Logger.Info("✅ Message emitted", "stream", stream)
			}
		}
	}()

	return ch
}

// EmitterPulling continuously executes a function and emits its output.
func (s *Service) EmitterPulling(stream string, fetch func() ([]byte, error), interval time.Duration) {
	go func() {
		for {
			start := time.Now()
			msg, err := fetch()
			if err != nil {
				s.Logger.Error("❌ EmitterPulling error", "stream", stream, "error", err)
			} else {
				s.Metrics.Increment("emitter_pulling")
				s.Metrics.ObserveDuration("emitter_pulling_time", start)
				s.ComplianceReporter.TrackEvent("emitter_pulling")
				if err := s.API.Publish(stream, msg); err != nil {
					s.Logger.Error("❌ Failed to publish pulled message", "stream", stream, "error", err)
				}
			}
			time.Sleep(interval)
		}
	}()
}

// Receiver subscribes to a transport stream and processes incoming messages.
func (s *Service) Receiver(stream string, handler func(ctx context.Context, data []byte)) {
	go func() {
		if err := s.API.Subscribe(stream, func(ctx context.Context, msg []byte) {
			s.Metrics.Increment("receiver_messages")
			s.ComplianceReporter.TrackEvent("receiver_messages")
			handler(ctx, msg)
		}); err != nil {
			s.Logger.Error("❌ Failed to subscribe to stream", "stream", stream, "error", err)
		}
	}()
}
