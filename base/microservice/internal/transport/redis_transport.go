package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/caaspay/caaspay-core/internal/logging"
	"github.com/caaspay/caaspay-core/internal/metrics"
	"github.com/caaspay/caaspay-core/pkg/api"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisTransport implements the Transport interface using Redis Streams.
type RedisTransport struct {
	client             redis.Cmdable
	useCompression     bool
	useEncryption      bool
	serviceReplyStream string
	encryptionKey      []byte

	maxRetries      int
	retryDelay      time.Duration
	blockTimeout    time.Duration
	readTimeout     time.Duration
	writeTimeout    time.Duration
	streamReadCount int64
	dlqStream       string

	ctx        context.Context
	logger     *logging.Logger
	metrics    *metrics.Metrics
	supervisor api.SupervisorInterface
}

// RedisTransportConfig defines the configuration for RedisTransport.
type RedisTransportConfig struct {
	RedisAddr          []string      // Redis node addresses (cluster or single-node)
	UseCluster         bool          // Use redis cluster client
	TLSRequired        bool          // Use TLS when connecting
	UseCompression     bool          // Enable compression of message payloads
	UseEncryption      bool          // Enable encryption of message payloads
	ServiceReplyStream string        // Stream used for service RPC responses
	EncryptionKey      string        // Encryption key (AES-GCM)
	MaxRetries         int           // Max number of retries on failure (default 3)
	RetryDelay         time.Duration // Delay between retries (default 500ms)
	DLQStream          string        // Optional: stream name for dead-letter queue
	PoolSize           int           // Max number of Redis connections
	MinIdleConns       int           // Minimum idle connections in pool
	DialTimeout        time.Duration // Timeout for establishing new connections
	ReadTimeout        time.Duration // Timeout for socket reads
	WriteTimeout       time.Duration // Timeout for socket writes
	StreamReadCount    int64         // Number of messages to read from stream at once
}

func NewRedisTransport(ctx context.Context, logger *logging.Logger, metrics *metrics.Metrics, sup api.SupervisorInterface, cfg RedisTransportConfig) (*RedisTransport, error) {
	var client redis.Cmdable

	// Default timeouts
	dialTimeout := cfg.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = 5 * time.Second
	}
	readTimeout := cfg.ReadTimeout
	if readTimeout == 0 {
		readTimeout = 3 * time.Second
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout == 0 {
		writeTimeout = 3 * time.Second
	}
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 300
	}
	retryDelay := cfg.RetryDelay
	if retryDelay <= 0 {
		retryDelay = 1000 * time.Millisecond
	}
	// TLS handling
	var tlsConfig *tls.Config
	if cfg.TLSRequired {
		tlsConfig = &tls.Config{InsecureSkipVerify: false}
	}

	// Redis client setup
	if cfg.UseCluster {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        cfg.RedisAddr,
			TLSConfig:    tlsConfig,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConns,
			DialTimeout:  dialTimeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
		})
	} else {
		addr := "localhost:6379"
		if len(cfg.RedisAddr) > 0 {
			addr = cfg.RedisAddr[0]
		}
		client = redis.NewClient(&redis.Options{
			Addr:         addr,
			TLSConfig:    tlsConfig,
			PoolSize:     cfg.PoolSize,
			MinIdleConns: cfg.MinIdleConns,
			DialTimeout:  dialTimeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
		})
	}

	// Dead Letter Queue fallback
	dlqStream := cfg.DLQStream
	if dlqStream == "" {
		dlqStream = "dlq:" + cfg.ServiceReplyStream
	}

	rt := &RedisTransport{
		client:             client,
		useCompression:     cfg.UseCompression,
		useEncryption:      cfg.UseEncryption,
		serviceReplyStream: cfg.ServiceReplyStream,
		encryptionKey:      []byte(cfg.EncryptionKey),
		maxRetries:         maxRetries,
		retryDelay:         retryDelay,
		dlqStream:          dlqStream,
		logger:             logger,
		metrics:            metrics,
		ctx:                ctx,
		supervisor:         sup,
		blockTimeout:       readTimeout,
		readTimeout:        readTimeout,
		writeTimeout:       writeTimeout,
		streamReadCount:    cfg.StreamReadCount,
	}

	// Check connection once at startup
	// Retry connection verification with backoff
	var trials int
	for {
		trials++
		if err := rt.verifyConnection(); err != nil {
			logger.Error("⏳ RedisTransport connection failed, retrying...", map[string]interface{}{

				"error": err.Error(),
				"trial": trials,
				"delay": rt.retryDelay * time.Duration(trials),
			})
			if trials >= maxRetries {

				logger.Error("❌ RedisTransport connection verification failed", map[string]interface{}{
					"error": err.Error(),
					"trial": trials,
				})
				return nil, fmt.Errorf("failed to connect to Redis: %w", err)
			}
			time.Sleep(rt.retryDelay * time.Duration(trials))
			continue
		}

		logger.Info("✅ RedisTransport connected successfully", map[string]interface{}{
			"cluster":     cfg.UseCluster,
			"pool_size":   cfg.PoolSize,
			"min_idle":    cfg.MinIdleConns,
			"compression": cfg.UseCompression,
			"encryption":  cfg.UseEncryption,
		})
		break
	}
	return rt, nil
}

func (r *RedisTransport) Close() error {
	switch client := r.client.(type) {
	case *redis.Client:
		return client.Close()
	case *redis.ClusterClient:
		return client.Close()
	default:
		return fmt.Errorf("Cannot close; unknown Redis client type")
	}
}

// IsHealthy returns true if Redis PING succeeds.
func (r *RedisTransport) IsHealthy() bool {
	ctx, cancel := context.WithTimeout(r.ctx, r.readTimeout)
	defer cancel()
	return r.client.Ping(ctx).Err() == nil
}

// verifyConnection checks broker reachability.
func (r *RedisTransport) verifyConnection() error {
	ctx, cancel := context.WithTimeout(r.ctx, r.writeTimeout)
	defer cancel()
	return r.client.Ping(ctx).Err()
}

// Request sends an RPC request and waits for a response.
func (r *RedisTransport) Request(stream string, data []byte, timeout time.Duration) ([]byte, error) {
	replyStream := fmt.Sprintf("reply:%s", uuid.New().String())
	responseCh := make(chan []byte, 1)

	r.supervisor.Go("redis_listenForReply", func(ctx context.Context) error {
		r.listenForReply(replyStream, responseCh, timeout)
		return nil
	})

	encodedData, err := r.prepareData(data)
	if err != nil {
		r.logger.Error("prepareData error in Request", map[string]interface{}{"error": err.Error()})
		return nil, err
	}

	addOp := func() error {
		return r.client.XAdd(r.ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{
				"body":     encodedData,
				"reply_to": replyStream,
			},
		}).Err()
	}

	if err := Retry(r.maxRetries, r.retryDelay, addOp); err != nil {
		r.logger.Error("Failed to send request after retries", map[string]interface{}{"error": err.Error()})
		return nil, fmt.Errorf("failed to send request after retries: %w", err)
	}

	r.logger.Info("Sending RPC request", map[string]interface{}{
		"stream": stream,
		"size":   len(data),
	})

	select {
	case response := <-responseCh:
		return response, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("request timeout")
	}
}

func (r *RedisTransport) listenForReply(replyStream string, responseCh chan<- []byte, timeout time.Duration) {
	for {
		select {
		case <-r.ctx.Done():
			r.logger.Info("listenForReply: context canceled", nil)
			return
		default:
			res, err := r.client.XRead(r.ctx, &redis.XReadArgs{
				Streams: []string{replyStream, "0"},
				Count:   1,
				Block:   timeout,
			}).Result()
			if err != nil {
				if r.ctx.Err() != nil {
					return
				}
				r.logger.Error("Error reading reply", map[string]interface{}{"error": err.Error()})
				continue
			}
			if len(res) > 0 && len(res[0].Messages) > 0 {
				msg := res[0].Messages[0]
				body, ok := msg.Values["body"].(string)
				if !ok {
					r.logger.Error("listenForReply: invalid message format", nil)
					continue
				}
				processed, err := r.processData([]byte(body))
				if err != nil {
					r.logger.Error("processData error in listenForReply", map[string]interface{}{"error": err.Error()})
				}
				responseCh <- processed
				return
			}
		}
	}
}

func (r *RedisTransport) Publish(stream string, data []byte) error {
	encodedData, err := r.prepareData(data)
	if err != nil {
		r.logger.Error("prepareData error in Publish", map[string]interface{}{"error": err.Error()})
		return err
	}
	addOp := func() error {
		_, err := r.client.XAdd(r.ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"body": encodedData},
		}).Result()
		return err
	}
	if err := Retry(r.maxRetries, r.retryDelay, addOp); err != nil {
		r.logger.Error("Failed to publish message after retries", map[string]interface{}{"error": err.Error()})
		if dlqErr := PublishToDLQ(r.ctx, r.client, r.dlqStream, stream, encodedData); dlqErr != nil {
			r.logger.Error("Failed to publish to DLQ", map[string]interface{}{"error": dlqErr.Error()})
		}
		return fmt.Errorf("failed to publish message after retries: %w", err)
	}

	r.logger.Info("Publishing message", map[string]interface{}{
		"stream": stream,
		"size":   len(data),
	})

	return nil
}

func (r *RedisTransport) Subscribe(consumerGroup string, stream string, handler api.HandlerFunc) error {
	group := "consumer_group"
	if consumerGroup != "" {
		group = consumerGroup
	}
	consumer := uuid.New().String()

	if err := r.client.XGroupCreateMkStream(r.ctx, stream, group, "$").Err(); err != nil {
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			r.logger.Error("Error creating consumer group", map[string]interface{}{"error": err.Error()})
		}
	}

	r.logger.Info("Subscribing to stream", map[string]interface{}{
		"stream": stream,
	})

	//	r.supervisor.Go("redis_subscribe_"+stream, func(ctx context.Context) error {
	for {
		select {
		case <-r.ctx.Done():
			r.logger.Info("Context canceled, exiting subscription loop", map[string]interface{}{"stream": stream})
			return nil
		default:
			res, err := r.client.XReadGroup(r.ctx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: consumer,
				Streams:  []string{stream, ">"},
				Count:    r.streamReadCount,
				Block:    r.blockTimeout,
			}).Result()

			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue // ⬅️ No message, try again
				}
				r.logger.Error("Error reading from stream", map[string]interface{}{"error": err.Error()})
				return err
			}

			for _, s := range res {
				for _, msg := range s.Messages {
					body, ok := msg.Values["body"].(string)
					if !ok {
						r.logger.Error("Invalid message body format", nil)
						r.client.XAck(r.ctx, s.Stream, group, msg.ID)
						continue
					}
					processed, err := r.processData([]byte(body))
					if err != nil {
						r.logger.Error("processData error", map[string]interface{}{"error": err.Error()})
					}
					var handlerErr error
					for attempt := 1; attempt <= r.maxRetries; attempt++ {
						_, handlerErr = handler(processed)
						if handlerErr == nil {
							break
						}
						r.logger.Error("Handler error", map[string]interface{}{
							"attempt": attempt,
							"msgID":   msg.ID,
							"error":   handlerErr.Error(),
						})
						time.Sleep(r.retryDelay)
					}
					if handlerErr == nil {
						r.client.XAck(r.ctx, s.Stream, group, msg.ID)
					} else {
						r.logger.Error("Handler failed after retries; sending to DLQ", map[string]interface{}{"msgID": msg.ID})
						if dlqErr := PublishToDLQ(r.ctx, r.client, r.dlqStream, stream, body); dlqErr != nil {
							r.logger.Error("Failed to publish to DLQ", map[string]interface{}{"error": dlqErr.Error()})
						}
						r.client.XAck(r.ctx, s.Stream, group, msg.ID)
					}
				}
			}
		}
	}
	//	})

	// return nil
}

func (r *RedisTransport) prepareData(data []byte) ([]byte, error) {
	var err error
	if r.useCompression {
		data, err = CompressZstd(data)
		if err != nil {
			return data, err
		}
	}
	if r.useEncryption {
		data, err = EncryptAES(data, r.encryptionKey)
		if err != nil {
			return data, err
		}
	}
	return data, nil
}

func (r *RedisTransport) processData(data []byte) ([]byte, error) {
	var err error
	if r.useEncryption {
		data, err = DecryptAES(data, r.encryptionKey)
		if err != nil {
			return data, err
		}
	}
	if r.useCompression {
		data, err = DecompressZstd(data)
		if err != nil {
			return data, err
		}
	}
	return data, nil
}
