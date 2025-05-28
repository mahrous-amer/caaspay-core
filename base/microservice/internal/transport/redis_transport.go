package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/caaspay/caaspay-core/internal/logging"
	"github.com/caaspay/caaspay-core/internal/metrics"
	"github.com/caaspay/caaspay-core/pkg/api"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisTransport implements the Transport interface using Redis Streams.
type RedisTransport struct {
	client                    redis.Cmdable
	redisCfg                  *RedisTransportConfig
	useCompression            bool
	useEncryption             bool
	serviceReplyStream        string
	responseOnServiceStream   bool
	subscribedToServiceStream sync.Once
	encryptionKey             string
	serviceInstanceID         string

	maxRetries      int
	retryDelay      time.Duration
	blockTimeout    time.Duration
	readTimeout     time.Duration
	writeTimeout    time.Duration
	streamReadCount int64
	dlqStream       string

	ctx             context.Context
	logger          *logging.Logger
	metrics         *metrics.Metrics
	supervisor      api.SupervisorInterface
	replyRouter     sync.Map // key: messageID, value: chan []byte
	streamsCreated  sync.Map // key: stream name (string), value: StreamMeta
	pendingCleanups sync.Map // key: stream+group string, value: bool
}

type StreamMeta struct {
	ToDelete       bool   // Whether to delete the stream on shutdown
	ToTrim         bool   // Whether to periodically trim this stream
	ToCleanPending bool   // whether to clean up pending messages from this stream
	Group          string // Consumer group name
	ToCleanGroup   bool   // whether to clean up the consumer group
	Overflowing    bool   // whether the stream is overflowing or not
}

// RedisTransportConfig defines the configuration for RedisTransport.
type RedisTransportConfig struct {
	RedisAddr               []string      // Redis node addresses (cluster or single-node)
	ServiceInstanceID       string        // Service instance ID
	UseCluster              bool          // Use redis cluster client
	TLSRequired             bool          // Use TLS when connecting
	UseCompression          bool          // Enable compression of message payloads
	UseEncryption           bool          // Enable encryption of message payloads
	ServiceReplyStream      string        // Stream used for service RPC responses
	ResponseOnServiceStream bool          // To use Service Stream for RPC responses
	EncryptionKey           string        // Encryption key (AES-GCM)
	MaxRetries              int           // Max number of retries on failure (default 3)
	RetryDelay              time.Duration // Delay between retries (default 500ms)
	DLQStream               string        // Optional: stream name for dead-letter queue
	MoveExpiredToDLQ        bool          // Move expired messages to DLQ
	PoolSize                int           // Max number of Redis connections
	MinIdleConns            int           // Minimum idle connections in pool
	DialTimeout             time.Duration // Timeout for establishing new connections
	ReadTimeout             time.Duration // Timeout for socket reads
	WriteTimeout            time.Duration // Timeout for socket writes
	StreamReadCount         int64         // Number of messages to read from stream at once
	StreamTrimMaxLen        int64         // hard cap on stream length (e.g., 10000 entries)
	StreamTrimApprox        bool          // use ~ approximation (faster trim)
	PeriodicTrimFreq        time.Duration // How often to run periodic trimming (0 disables)
}

// StreamStats holds metrics for a Redis stream.
type StreamStats struct {
	Name   string          `json:"name"`
	Length int64           `json:"length"`
	Groups []ConsumerGroup `json:"groups"`
}

type ConsumerGroup struct {
	Name      string           `json:"name"`
	Lag       int64            `json:"lag"`
	Pending   int64            `json:"pending"`
	Consumers []ConsumerDetail `json:"consumers"`
}

type ConsumerDetail struct {
	Name    string        `json:"name"`
	Pending int64         `json:"pending"`
	IdleMS  time.Duration `json:"idle_ms"`
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
		client:                  client,
		redisCfg:                &cfg,
		serviceInstanceID:       cfg.ServiceInstanceID,
		useCompression:          cfg.UseCompression,
		useEncryption:           cfg.UseEncryption,
		serviceReplyStream:      cfg.ServiceReplyStream,
		encryptionKey:           cfg.EncryptionKey,
		maxRetries:              maxRetries,
		retryDelay:              retryDelay,
		dlqStream:               dlqStream,
		logger:                  logger,
		metrics:                 metrics,
		ctx:                     ctx,
		supervisor:              sup,
		blockTimeout:            readTimeout,
		readTimeout:             readTimeout,
		writeTimeout:            writeTimeout,
		streamReadCount:         cfg.StreamReadCount,
		responseOnServiceStream: cfg.ResponseOnServiceStream,
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

	if rt.redisCfg.PeriodicTrimFreq > 0 && rt.redisCfg.StreamTrimMaxLen > 0 {
		sup.GoLoop("redis_periodic_trim", func(ctx context.Context) (time.Duration, error) {
			rt.streamsCreated.Range(func(key, value any) bool {
				stream := key.(string)
				meta, ok := value.(StreamMeta)
				if !ok || !meta.ToTrim {
					return true
				}

				stats, err := rt.collectStreamStats(stream)
				if err != nil {
					rt.logger.Warn("Failed to collect stream stats", map[string]interface{}{"stream": stream, "error": err.Error()})
					return true
				}

				// Determine if any consumer group is lagging badly
				overflowing := false
				for _, group := range stats.Groups {
					if group.Lag > rt.redisCfg.StreamTrimMaxLen {
						rt.logger.Warn("Stream is overflowing", map[string]interface{}{
							"stream": stream,
							"group":  group.Name,
							"lag":    group.Lag,
						})
						overflowing = true
						break
					}
				}

				// Update meta.Overflowing status
				meta.Overflowing = overflowing
				rt.streamsCreated.Store(stream, meta)

				// Only trim if it's not overflowing
				if !overflowing {
					rt.trimStream(stream)
				}

				return true
			})
			return rt.redisCfg.PeriodicTrimFreq, nil
		})
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
	r.streamsCreated.Range(func(key, value any) bool {
		stream := key.(string)
		//				meta, ok := value.(StreamMeta)
		//				if ok && meta.ToTrim {
		//					rt.trimStream(stream)
		//				}
		stats, _ := r.collectStreamStats(stream)
		r.logger.Info("stream stats", map[string]interface{}{"streamStats": stats})
		return true
	})

	return r.client.Ping(ctx).Err() == nil
}

// Add to RedisTransport
func (r *RedisTransport) registerStream(stream string, meta StreamMeta) {
	meta.Overflowing = false
	r.streamsCreated.Store(stream, meta)
}

func (r *RedisTransport) isStreamOverflowing(stream string) bool {
	if v, ok := r.streamsCreated.Load(stream); ok {
		if meta, ok := v.(StreamMeta); ok {
			return meta.Overflowing
		}
	}
	return false
}

// verifyConnection checks broker reachability.
func (r *RedisTransport) verifyConnection() error {
	ctx, cancel := context.WithTimeout(r.ctx, r.writeTimeout)
	defer cancel()
	return r.client.Ping(ctx).Err()
}

func (r *RedisTransport) ensureConsumerGroup(group, stream string, meta StreamMeta) error {
	err := r.client.XGroupCreateMkStream(r.ctx, stream, group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		r.logger.Error("Failed to create consumer group", map[string]interface{}{
			"stream": stream,
			"group":  group,
			"error":  err.Error(),
		})
		return err
	}

	// ✅ Track the stream for deletion or trimming
	//r.streamsCreated.LoadOrStore(stream, meta)
	meta.Group = group
	r.registerStream(stream, meta)

	// ✅ Start pending cleanup loop once
	if meta.ToCleanPending {
		key := stream + "::" + group
		if _, loaded := r.pendingCleanups.LoadOrStore(key, true); !loaded {
			// it has to happen separately not to delay the stream craetion/ensuring step
			r.supervisor.Go("cleanup_pending_"+group, func(ctx context.Context) error {
				return r.cleanupExpiredPendingMessages(stream, group)
			})
		}
	}

	return nil
}

func (r *RedisTransport) collectStreamStats(stream string) (*StreamStats, error) {
	if errors.Is(r.ctx.Err(), context.Canceled) {
		r.logger.Info("Skipping XInfoStream due to context cancellation", map[string]interface{}{"stream": stream})
		return nil, nil
	}
	// XINFO STREAM
	streamInfo, err := r.client.XInfoStream(r.ctx, stream).Result()
	if err != nil {
		r.logger.Warn("XInfoStream failed", map[string]interface{}{"stream": stream, "error": err.Error()})
		return nil, err
	}
	stats := &StreamStats{
		Name:   stream,
		Length: streamInfo.Length,
		Groups: []ConsumerGroup{},
	}

	// XINFO GROUPS
	groups, err := r.client.XInfoGroups(r.ctx, stream).Result()
	if err != nil {
		r.logger.Warn("XInfoGroups failed", map[string]interface{}{"stream": stream, "error": err.Error()})
		return stats, nil // continue with partial info
	}

	for _, grp := range groups {
		cg := ConsumerGroup{
			Name:    grp.Name,
			Lag:     grp.Lag,
			Pending: grp.Pending,
		}

		// XINFO CONSUMERS
		consumers, err := r.client.XInfoConsumers(r.ctx, stream, grp.Name).Result()
		if err != nil {
			r.logger.Warn("XInfoConsumers failed", map[string]interface{}{
				"stream": stream,
				"group":  grp.Name,
				"error":  err.Error(),
			})
			stats.Groups = append(stats.Groups, cg)
			continue
		}

		for _, c := range consumers {
			cg.Consumers = append(cg.Consumers, ConsumerDetail{
				Name:    c.Name,
				Pending: c.Pending,
				IdleMS:  c.Idle,
			})
		}
		stats.Groups = append(stats.Groups, cg)
	}

	return stats, nil
}

func (r *RedisTransport) trimStream(stream string) {
	if r.redisCfg.StreamTrimMaxLen <= 0 {
		return
	}
	args := []interface{}{"XTRIM", stream, "MAXLEN"}
	if r.redisCfg.StreamTrimApprox {
		args = append(args, "~")
	}
	args = append(args, r.redisCfg.StreamTrimMaxLen)

	var err error
	switch cli := r.client.(type) {
	case *redis.Client:
		err = cli.Do(r.ctx, args...).Err()
	case *redis.ClusterClient:
		err = cli.Do(r.ctx, args...).Err()
	default:
		r.logger.Warn("Unsupported Redis client type for stream trimming", map[string]interface{}{
			"stream": stream,
		})
		return
	}

	if err != nil {
		r.logger.Warn("Failed to trim stream", map[string]interface{}{
			"stream": stream,
			"error":  err.Error(),
		})
	}
}

func (r *RedisTransport) cleanupStaleConsumers(stream, group string, idleThreshold time.Duration) {
	consumers, err := r.client.XInfoConsumers(r.ctx, stream, group).Result()
	if err != nil {
		return
	}
	for _, c := range consumers {
		if c.Idle > idleThreshold {
			r.logger.Info("🧹 Removing stale consumer", map[string]interface{}{
				"consumer": c.Name, "idle_ms": c.Idle.Milliseconds(),
			})
			r.client.XGroupDelConsumer(r.ctx, stream, group, c.Name)
		}
	}
}

func (r *RedisTransport) CleanupOnShutdown() {
	r.streamsCreated.Range(func(key, value any) bool {
		stream := key.(string)
		meta := value.(StreamMeta)
		if meta.ToDelete {
			if err := r.client.Del(r.ctx, stream).Err(); err == nil {
				r.logger.Info("🔪 Deleted stream", map[string]interface{}{"stream": stream})
			}
		} else if meta.ToCleanGroup && meta.Group != "" {
			if err := r.client.XGroupDestroy(r.ctx, stream, meta.Group).Err(); err == nil {
				r.logger.Info("🧹 Deleted consumer group", map[string]interface{}{
					"stream": stream,
					"group":  meta.Group,
				})
			} else {
				r.logger.Warn("Failed to delete consumer group", map[string]interface{}{
					"stream": stream,
					"group":  meta.Group,
					"error":  err.Error(),
				})
			}
		}
		return true
	})
}

func (r *RedisTransport) CleanupOnStartup() {
	r.streamsCreated.Range(func(key, value any) bool {
		stream := key.(string)
		meta := value.(StreamMeta)
		if meta.ToCleanPending {
			r.cleanupStaleConsumers(stream, meta.Group, 30*time.Minute)
		}
		return true
	})
}

// Request sends an RPC request and waits for a response.
func (r *RedisTransport) Request(stream string, msg *api.TransportMessage, timeout time.Duration) ([]byte, error) {
	// ensure requested method and service exists
	exists, err := r.client.Exists(r.ctx, stream).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to verify stream existence: %w", err)
	}
	if exists == 0 {
		r.logger.Error("Requesting RPC stream that does not exists", map[string]interface{}{"stream": stream, "message": msg})
		return nil, fmt.Errorf("target RPC stream does not exist: %s", stream)
	}
	// Ensure ReplyTo is set
	if msg.ReplyTo == "" {
		if r.responseOnServiceStream {
			msg.ReplyTo = r.serviceReplyStream
		} else {
			msg.ReplyTo = fmt.Sprintf("reply:%s:%s", stream, r.serviceInstanceID)
		}
	}

	// Prepare response channel
	responseCh := make(chan []byte, 1)
	r.replyRouter.Store(msg.MessageID, responseCh)

	// Start listener goroutine
	r.subscribedToServiceStream.Do(func() {
		r.supervisor.Go("redis_listenForReply", func(ctx context.Context) error {
			r.listenForReply(msg.ReplyTo, timeout)
			return nil
		})
	})

	// Encode message
	msgBytes, err := msg.Encode()
	if err != nil {
		r.logger.Error("failed to encode TransportMessage", map[string]interface{}{"error": err.Error()})
		return nil, err
	}

	// Encrypt/compress if enabled
	encodedData, err := r.prepareData(msgBytes)
	if err != nil {
		r.logger.Error("prepareData error in Request", map[string]interface{}{"error": err.Error()})
		return nil, err
	}

	// Send to Redis stream
	r.logger.Info("Sending RPC request", map[string]interface{}{
		"stream": stream,
		"size":   len(encodedData),
	})
	addOp := func() error {
		return r.client.XAdd(r.ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{
				"body": encodedData,
			},
		}).Err()
	}

	if err := Retry(r.ctx, r.maxRetries, r.retryDelay, addOp); err != nil {
		r.logger.Error("Failed to send request after retries", map[string]interface{}{"error": err.Error()})
		return nil, fmt.Errorf("failed to send request after retries: %w", err)
	}

	// Wait for reply
	select {
	case response := <-responseCh:
		return response, nil
	case <-time.After(timeout):
		r.replyRouter.Delete(msg.MessageID)
		return nil, fmt.Errorf("request timeout")
	}
}

func (r *RedisTransport) listenForReply(replyStream string, timeout time.Duration) {
	const groupName = "rpc_cg"
	//consumerName := uuid.New().String()
	consumerName := r.serviceInstanceID
	// Ensure consumer group exists (MKSTREAM allows stream autocreation)
	if err := r.ensureConsumerGroup(groupName, replyStream, StreamMeta{ToDelete: true, ToTrim: true, ToCleanPending: true, ToCleanGroup: true}); err != nil {
		r.logger.Error("Failed to create consumer group", map[string]interface{}{
			"stream": replyStream,
			"group":  groupName,
			"error":  err.Error(),
		})
		return
	}
	for {
		select {
		case <-r.ctx.Done():
			r.logger.Info("listenForReply: context canceled", nil)
			return
		default:
			res, err := r.client.XReadGroup(r.ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{replyStream, ">"},
				Count:    1,
				Block:    r.blockTimeout,
			}).Result()
			if err != nil {
				if r.ctx.Err() != nil {
					return
				}
				r.logger.Error("Error reading reply (XREADGROUP)", map[string]interface{}{"error": err.Error()})
				continue
			}

			if len(res) > 0 && len(res[0].Messages) > 0 {
				msg := res[0].Messages[0]
				messageID := msg.ID

				body, ok := msg.Values["body"].(string)
				if !ok {
					r.logger.Error("listenForReply: invalid message format", nil)
					continue
				}

				processed, err := r.processData([]byte(body))
				if err != nil {
					r.logger.Error("processData error in listenForReply", map[string]interface{}{"error": err.Error()})
					continue
				}

				decoded, err := api.DecodeTransportMessage(processed)
				if err != nil {
					r.logger.Error("Failed to decode response message", map[string]interface{}{"error": err.Error()})
					continue
				}

				if decoded.Deadline > 0 && time.Now().After(time.Unix(0, decoded.Deadline)) {
					r.logger.Warn("RPC response past deadline, discarding", map[string]interface{}{
						"messageID": decoded.MessageID,
						"replyTo":   replyStream,
					})
					if r.redisCfg.MoveExpiredToDLQ {
						_ = PublishToDLQ(r.ctx, r.client, r.dlqStream, replyStream, body)
					}
					r.client.XAck(r.ctx, replyStream, groupName, msg.ID)
					continue
				}

				// ✅ Send reply to waiting channel
				if responseCh, ok := r.replyRouter.LoadAndDelete(decoded.MessageID); ok {
					responseCh.(chan []byte) <- processed
					// ✅ Acknowledge the message
					if err := r.client.XAck(r.ctx, replyStream, groupName, messageID).Err(); err != nil {
						r.logger.Warn("Failed to acknowledge reply message", map[string]interface{}{
							"stream":     replyStream,
							"group":      groupName,
							"message_id": messageID,
							"error":      err.Error(),
						})
					}
				} else {
					r.logger.Warn("🔍 Stray RPC response unknown MessageID", map[string]interface{}{
						"messageID": decoded.MessageID,
						"replyTo":   replyStream,
					})
				}
			}
		}
	}
}

func (r *RedisTransport) cleanupExpiredPendingMessages(stream, group string) error {
	pendingRes, err := r.client.XPending(r.ctx, stream, group).Result()
	if err != nil {
		r.logger.Warn("Failed XPENDING", map[string]interface{}{"stream": stream, "error": err.Error()})
		return nil
	}
	if pendingRes.Count == 0 {
		return nil
	}

	entries, err := r.client.XPendingExt(r.ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		r.logger.Warn("Failed XPendingExt", map[string]interface{}{"stream": stream, "error": err.Error()})
		return nil
	}

	for _, entry := range entries {
		msgID := entry.ID
		msgs, err := r.client.XRange(r.ctx, stream, msgID, msgID).Result()
		if err != nil || len(msgs) == 0 {
			continue
		}
		bodyStr, ok := msgs[0].Values["body"].(string)
		if !ok {
			continue
		}
		processed, err := r.processData([]byte(bodyStr))
		if err != nil {
			continue
		}
		decoded, err := api.DecodeTransportMessage(processed)
		if err != nil {
			continue
		}
		if decoded.Deadline > 0 && time.Now().After(time.Unix(0, decoded.Deadline)) {
			r.logger.Warn("Removing expired pending message", map[string]interface{}{
				"stream": stream,
				"msgID":  msgID,
			})
			r.client.XAck(r.ctx, stream, group, msgID)
			if r.redisCfg.MoveExpiredToDLQ {
				_ = PublishToDLQ(r.ctx, r.client, r.dlqStream, stream, bodyStr)
			}
		}
	}
	return nil
}

func (r *RedisTransport) Publish(ctx context.Context, stream string, data []byte) error {
	encodedData, err := r.prepareData(data)
	if err != nil {
		r.logger.Error("prepareData error in Publish", map[string]interface{}{"error": err.Error()})
		return err
	}
	r.registerStream(stream, StreamMeta{
		//ToTrim: r.redisCfg.StreamTrimMaxLen > 0,
		ToTrim: true,
	})
	if r.isStreamOverflowing(stream) {
		return fmt.Errorf("stream %s is currently overflowing, throttling in place", stream)
	}
	addOp := func() error {
		_, err := r.client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"body": encodedData},
		}).Result()
		return err
	}
	if err := Retry(ctx, r.maxRetries, r.retryDelay, addOp); err != nil {
		r.logger.Error("Failed to publish message after retries", map[string]interface{}{"error": err.Error()})
		if dlqErr := PublishToDLQ(ctx, r.client, r.dlqStream, stream, encodedData); dlqErr != nil {
			r.logger.Error("Failed to publish to DLQ", map[string]interface{}{"error": dlqErr.Error()})
		}
		return fmt.Errorf("failed to publish message after retries: %w", err)
	}

	r.logger.Trace("Publishing message", map[string]interface{}{
		"stream": stream,
		"size":   len(data),
	})

	return nil
}

func (r *RedisTransport) Emit(stream string, msg *api.TransportMessage) error {
	encoded, err := msg.Encode()
	if err != nil {
		r.logger.Error("Emit: failed to encode message", map[string]interface{}{
			"stream": stream,
			"error":  err.Error(),
		})
		return err
	}
	return r.Publish(r.ctx, stream, encoded)
}

func (r *RedisTransport) Subscribe(consumerGroup string, stream string, handler api.HandlerFunc) error {
	group := "consumer_group"
	if consumerGroup != "" {
		group = consumerGroup
	}
	consumer := uuid.New().String()

	if err := r.ensureConsumerGroup(group, stream, StreamMeta{ToDelete: false, ToTrim: true, ToCleanPending: true, ToCleanGroup: false}); err != nil {
		r.logger.Error("Failed to create consumer group", map[string]interface{}{
			"stream": stream,
			"group":  group,
			"error":  err.Error(),
		})
		return err
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
