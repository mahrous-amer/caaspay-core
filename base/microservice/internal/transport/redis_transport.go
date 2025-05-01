package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/caaspay/caaspay-core/internal/logging"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisTransport implements the Transport interface using Redis Streams.
type RedisTransport struct {
	client             *redis.Client
	useCompression     bool
	useEncryption      bool
	serviceReplyStream string
	encryptionKey      []byte

	maxRetries int
	retryDelay time.Duration
	dlqStream  string

	logger *logging.Logger
}

// RedisTransportConfig defines the configuration for RedisTransport.
type RedisTransportConfig struct {
	RedisAddr          string
	UseCompression     bool
	UseEncryption      bool
	ServiceReplyStream string
	EncryptionKey      string
	MaxRetries         int
	RetryDelay         time.Duration
	DLQStream          string
}

// NewRedisTransport initializes a RedisTransport with the provided configuration and logger.
func NewRedisTransport(cfg RedisTransportConfig, logger *logging.Logger) *RedisTransport {
	client := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		// Enterprise: can configure poolSize, minIdleConns, etc., from cfg.
	})

	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	retryDelay := cfg.RetryDelay
	if retryDelay <= 0 {
		retryDelay = 500 * time.Millisecond
	}
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
	}

	if err := rt.verifyConnection(); err != nil {
		logger.Error(context.Background(), "RedisTransport connection verification failed", map[string]interface{}{"error": err.Error()})
	}

	return rt
}

// IsHealthy returns true if Redis PING succeeds.
func (r *RedisTransport) IsHealthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return r.client.Ping(ctx).Err() == nil
}

// verifyConnection checks broker reachability.
func (r *RedisTransport) verifyConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return r.client.Ping(ctx).Err()
}

// Request sends an RPC request and waits for a response.
func (r *RedisTransport) Request(ctx context.Context, stream string, data []byte, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	replyStream := fmt.Sprintf("reply:%s", uuid.New().String())
	responseCh := make(chan []byte, 1)

	go r.listenForReply(ctx, replyStream, responseCh)

	encodedData, err := r.prepareData(data)
	if err != nil {
		r.logger.Error(ctx, "prepareData error in Request", map[string]interface{}{"error": err.Error()})
		return nil, err
	}

	addOp := func() error {
		return r.client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{
				"body":     encodedData,
				"reply_to": replyStream,
			},
		}).Err()
	}

	if err := Retry(r.maxRetries, r.retryDelay, addOp); err != nil {
		r.logger.Error(ctx, "Failed to send request after retries", map[string]interface{}{"error": err.Error()})
		return nil, fmt.Errorf("failed to send request after retries: %w", err)
	}

	r.logger.Info(ctx, "Sending RPC request", map[string]interface{}{
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

func (r *RedisTransport) listenForReply(ctx context.Context, replyStream string, responseCh chan<- []byte) {
	for {
		select {
		case <-ctx.Done():
			r.logger.Info(ctx, "listenForReply: context canceled", nil)
			return
		default:
			res, err := r.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{replyStream, "0"},
				Count:   1,
				Block:   5 * time.Second,
			}).Result()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				r.logger.Error(ctx, "Error reading reply", map[string]interface{}{"error": err.Error()})
				continue
			}
			if len(res) > 0 && len(res[0].Messages) > 0 {
				msg := res[0].Messages[0]
				body, ok := msg.Values["body"].(string)
				if !ok {
					r.logger.Error(ctx, "listenForReply: invalid message format", nil)
					continue
				}
				processed, err := r.processData([]byte(body))
				if err != nil {
					r.logger.Error(ctx, "processData error in listenForReply", map[string]interface{}{"error": err.Error()})
				}
				responseCh <- processed
				return
			}
		}
	}
}

func (r *RedisTransport) Publish(ctx context.Context, stream string, data []byte) error {
	encodedData, err := r.prepareData(data)
	if err != nil {
		r.logger.Error(ctx, "prepareData error in Publish", map[string]interface{}{"error": err.Error()})
		return err
	}
	addOp := func() error {
		_, err := r.client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"body": encodedData},
		}).Result()
		return err
	}
	if err := Retry(r.maxRetries, r.retryDelay, addOp); err != nil {
		r.logger.Error(ctx, "Failed to publish message after retries", map[string]interface{}{"error": err.Error()})
		if dlqErr := PublishToDLQ(ctx, r.client, r.dlqStream, stream, encodedData); dlqErr != nil {
			r.logger.Error(ctx, "Failed to publish to DLQ", map[string]interface{}{"error": dlqErr.Error()})
		}
		return fmt.Errorf("failed to publish message after retries: %w", err)
	}

	r.logger.Info(ctx, "Publishing message", map[string]interface{}{
		"stream": stream,
		"size":   len(data),
	})

	return nil
}

func (r *RedisTransport) Subscribe(stream string, handler HandlerFunc) error {
	ctx := context.Background()
	group := "consumer_group"
	consumer := uuid.New().String()

	if err := r.client.XGroupCreateMkStream(ctx, stream, group, "$").Err(); err != nil {
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			r.logger.Error(ctx, "Error creating consumer group", map[string]interface{}{"error": err.Error()})
		}
	}

	r.logger.Info(ctx, "Subscribing to stream", map[string]interface{}{
		"stream": stream,
	})

	for {
		res, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    0,
		}).Result()
		if err != nil {
			r.logger.Error(ctx, "Error reading from stream", map[string]interface{}{"error": err.Error()})
			continue
		}
		for _, s := range res {
			for _, msg := range s.Messages {
				body, ok := msg.Values["body"].(string)
				if !ok {
					r.logger.Error(ctx, "Subscribe: invalid message body format", nil)
					r.client.XAck(ctx, s.Stream, group, msg.ID)
					continue
				}
				processed, err := r.processData([]byte(body))
				if err != nil {
					r.logger.Error(ctx, "processData error in Subscribe", map[string]interface{}{"error": err.Error()})
				}
				var handlerErr error
				for attempt := 1; attempt <= r.maxRetries; attempt++ {
					_, handlerErr = handler(ctx, processed)
					if handlerErr == nil {
						break
					}
					r.logger.Error(ctx, "Handler error", map[string]interface{}{
						"attempt": attempt,
						"msgID":   msg.ID,
						"error":   handlerErr.Error(),
					})
					time.Sleep(r.retryDelay)
				}
				if handlerErr == nil {
					r.client.XAck(ctx, s.Stream, group, msg.ID)
				} else {
					r.logger.Error(ctx, "Handler failed after retries; sending to DLQ", map[string]interface{}{"msgID": msg.ID})
					if dlqErr := PublishToDLQ(ctx, r.client, r.dlqStream, stream, body); dlqErr != nil {
						r.logger.Error(ctx, "Failed to publish to DLQ", map[string]interface{}{"error": dlqErr.Error()})
					}
					r.client.XAck(ctx, s.Stream, group, msg.ID)
				}
			}
		}
	}
}

func (r *RedisTransport) Close() error {
	return r.client.Close()
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
