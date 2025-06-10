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
	//	"github.com/google/uuid"
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
	cancelFunc      context.CancelFunc
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
	SubscribedTo   bool   // whether service is listening on this stream or not
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
	ResponseStreamSubOnce   bool          // Subscribe to response stream once throughout or with every request once
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

func NewRedisTransport(RootCtx context.Context, logger *logging.Logger, metrics *metrics.Metrics, sup api.SupervisorInterface, cfg RedisTransportConfig) (*RedisTransport, error) {
	var client redis.Cmdable

	ctx, cancel := context.WithCancel(RootCtx)
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
		cancelFunc:              cancel,
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
			logger.Error(ctx, "⏳ RedisTransport connection failed, retrying...", map[string]interface{}{

				"error": err.Error(),
				"trial": trials,
				"delay": rt.retryDelay * time.Duration(trials),
			})
			if trials >= maxRetries {

				logger.Error(ctx, "❌ RedisTransport connection verification failed", map[string]interface{}{
					"error": err.Error(),
					"trial": trials,
				})
				return nil, fmt.Errorf("failed to connect to Redis: %w", err)
			}
			time.Sleep(rt.retryDelay * time.Duration(trials))
			continue
		}

		logger.Info(ctx, "✅ RedisTransport connected successfully", map[string]interface{}{
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
					rt.logger.Warn(ctx, "Failed to collect stream stats", map[string]interface{}{"stream": stream, "error": err.Error()})
					return true
				}

				// Determine if any consumer group is lagging badly
				overflowing := false
				for _, group := range stats.Groups {
					if group.Lag > rt.redisCfg.StreamTrimMaxLen {
						rt.logger.Warn(ctx, "Stream is overflowing", map[string]interface{}{
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
	r.logger.Info(r.ctx, "🔒 Closing RedisTransport", nil)

	// Cancel internal context to stop any background routines
	if r.cancelFunc != nil {
		r.cancelFunc()
	}

	// Close Redis client
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
	// Apply read timeout only if parent has no deadline
	var ctx context.Context
	var cancel context.CancelFunc
	if _, hasDeadline := r.ctx.Deadline(); !hasDeadline {
		ctx, cancel = context.WithTimeout(r.ctx, r.readTimeout)
		defer cancel()
	}

	r.streamsCreated.Range(func(key, value any) bool {
		stream := key.(string)
		meta, ok := value.(StreamMeta)
		if !ok {
			return true
		}

		// Skip trimming and stats if context is already canceled
		if ctx.Err() != nil {
			r.logger.Info(r.ctx, "⏭️ Skipping health check for stream due to context cancellation", map[string]interface{}{
				"stream": stream,
			})
			return false
		}

		if meta.ToTrim {
			r.trimStream(stream)
		}

		stats, _ := r.collectStreamStats(stream)
		r.logger.Info(ctx, "📊 Stream stats", map[string]interface{}{
			"stream": stream,
			"stats":  stats,
		})

		return true
	})

	if err := r.client.Ping(ctx).Err(); err != nil {
		r.logger.Warn(ctx, "❌ Redis PING failed", map[string]interface{}{"error": err.Error()})
		return false
	}

	return true
}

// Add to RedisTransport
func (r *RedisTransport) registerStream(stream string, meta StreamMeta) StreamMeta {
	// Only set default values if they're not already defined
	if !meta.Overflowing {
		meta.Overflowing = false
	}
	if !meta.SubscribedTo {
		meta.SubscribedTo = false
	}

	// Store only if not already present
	actual, _ := r.streamsCreated.LoadOrStore(stream, meta)

	// Return what is actually stored (could be existing or just saved)
	if existingMeta, ok := actual.(StreamMeta); ok {
		return existingMeta
	}

	// Fallback (should never hit unless bad type stored)
	return meta
}

func (r *RedisTransport) isStreamOverflowing(stream string) bool {
	if v, ok := r.streamsCreated.Load(stream); ok {
		if meta, ok := v.(StreamMeta); ok {
			return meta.Overflowing
		}
	}
	return false
}

func (r *RedisTransport) isSubscribedToStream(stream string) bool {
	if v, ok := r.streamsCreated.Load(stream); ok {
		if meta, ok := v.(StreamMeta); ok {
			return meta.SubscribedTo
		}
	}
	return false
}

func (r *RedisTransport) updateStreamSubscriptionStatus(stream string, status bool) {
	for {
		v, ok := r.streamsCreated.Load(stream)
		if !ok {
			return
		}
		meta, ok := v.(StreamMeta)
		if !ok {
			return
		}
		meta.SubscribedTo = status
		// Optimistic locking
		if r.streamsCreated.CompareAndSwap(stream, v, meta) {
			return
		}
	}
}

// verifyConnection checks broker reachability.
func (r *RedisTransport) verifyConnection() error {
	ctx := r.ctx
	if _, hasDeadline := r.ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.writeTimeout)
		defer cancel()
	}

	if err := r.client.Ping(ctx).Err(); err != nil {
		r.logger.Warn(ctx, "❌ Redis ping failed during verifyConnection", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	r.logger.Debug(ctx, "✅ Redis ping successful in verifyConnection", nil)
	return nil
}

func (r *RedisTransport) ensureConsumerGroup(group, stream string, meta StreamMeta) error {
	ctx := r.ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.writeTimeout)
		defer cancel()
	}

	err := r.client.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			// Group already exists — safe to continue
		} else if strings.Contains(err.Error(), "NOGROUP") {
			// Retry once after short delay
			time.Sleep(50 * time.Millisecond)
			err = r.client.XGroupCreateMkStream(ctx, stream, group, "$").Err()
			if err != nil {
				// Final fallback log
				r.logger.Error(ctx, "❌ Failed to create consumer group after retry", map[string]interface{}{
					"stream": stream,
					"group":  group,
					"error":  err.Error(),
				})
				return err
			}
		} else {
			r.logger.Error(ctx, "❌ Failed to create consumer group", map[string]interface{}{
				"stream": stream,
				"group":  group,
				"error":  err.Error(),
			})
			return err
		}
	}

	// ✅ Register the stream and group metadata
	meta.Group = group
	meta = r.registerStream(stream, meta)

	// 🧹 Launch pending cleanup only once per stream-group
	if meta.ToCleanPending {
		key := stream + "::" + group
		if _, loaded := r.pendingCleanups.LoadOrStore(key, true); !loaded {
			r.logger.Info(ctx, "🧼 Starting pending cleanup for group", map[string]interface{}{
				"stream": stream,
				"group":  group,
			})

			// it has to happen separately not to delay the stream craetion/ensuring step
			r.supervisor.Go("cleanup_pending_"+group, func(innerCtx context.Context) error {
				return r.cleanupExpiredPendingMessages(ctx, stream, group)
			})
		}
	}

	r.logger.Debug(ctx, "✅ Consumer group ensured", map[string]interface{}{
		"stream": stream,
		"group":  group,
	})

	return nil
}

func (r *RedisTransport) collectStreamStats(stream string) (*StreamStats, error) {
	// Check if context is cancelled
	if r.ctx.Err() != nil {
		r.logger.Info(r.ctx, "Skipping XInfoStream due to context cancellation", map[string]interface{}{"stream": stream})
		return nil, r.ctx.Err()
	}

	// XINFO STREAM
	streamInfo, err := r.client.XInfoStream(r.ctx, stream).Result()
	if err != nil {
		r.logger.Warn(r.ctx, "XInfoStream failed", map[string]interface{}{"stream": stream, "error": err.Error()})
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
		r.logger.Warn(r.ctx, "XInfoGroups failed", map[string]interface{}{"stream": stream, "error": err.Error()})
		return stats, nil // proceed with partial info
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
			r.logger.Warn(r.ctx, "XInfoConsumers failed", map[string]interface{}{
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
	if r.ctx.Err() != nil {
		r.logger.Info(r.ctx, "⏭️ Skipping XTRIM due to context cancellation", map[string]interface{}{
			"stream": stream,
		})
		return
	}

	args := []interface{}{"XTRIM", stream, "MAXLEN"}
	if r.redisCfg.StreamTrimApprox {
		args = append(args, "~")
	}
	args = append(args, r.redisCfg.StreamTrimMaxLen)

	var (
		trimmed int64
		err     error
	)

	switch cli := r.client.(type) {
	case *redis.Client:
		trimmed, err = cli.Do(r.ctx, args...).Int64()
	case *redis.ClusterClient:
		trimmed, err = cli.Do(r.ctx, args...).Int64()
	default:
		r.logger.Warn(r.ctx, "❌ Unsupported Redis client type for stream trimming", map[string]interface{}{
			"stream": stream,
		})
		return
	}

	if err != nil {
		r.logger.Warn(r.ctx, "🚫 Failed to trim stream", map[string]interface{}{
			"stream": stream,
			"error":  err.Error(),
		})
		return
	}

	r.logger.Info(r.ctx, "✅ Trimmed Redis stream", map[string]interface{}{
		"stream":          stream,
		"entries_removed": trimmed,
	})

	// Optional: Add to metrics
	if r.metrics != nil {
		r.metrics.ObserveHistogram("redis.stream.trimmed", float64(trimmed), "stream", stream)
	}
}

func (r *RedisTransport) cleanupStaleConsumers(stream, group string, idleThreshold time.Duration) {
	// Check if context is cancelled
	if r.ctx.Err() != nil {
		r.logger.Info(r.ctx, "Skipping cleanupStaleConsumers due to context cancellation", map[string]interface{}{"stream": stream, "group": group})
		return
	}
	ctx := r.ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.writeTimeout)
		defer cancel()
	}
	consumers, err := r.client.XInfoConsumers(ctx, stream, group).Result()
	if err != nil {
		r.logger.Warn(ctx, "❌ Failed to fetch consumers for cleanup", map[string]interface{}{
			"stream": stream,
			"group":  group,
			"error":  err.Error(),
		})
		return
	}

	for _, c := range consumers {
		if c.Idle > idleThreshold {
			r.logger.Info(ctx, "🧹 Removing stale consumer", map[string]interface{}{
				"stream":   stream,
				"group":    group,
				"consumer": c.Name,
				"idle_ms":  c.Idle.Milliseconds(),
			})

			if err := r.client.XGroupDelConsumer(ctx, stream, group, c.Name).Err(); err != nil {
				r.logger.Warn(ctx, "⚠️ Failed to remove stale consumer", map[string]interface{}{
					"stream":   stream,
					"group":    group,
					"consumer": c.Name,
					"error":    err.Error(),
				})
			}
		}
	}
}

func (r *RedisTransport) CleanupOnShutdown() {
	r.streamsCreated.Range(func(key, value any) bool {

		stream := key.(string)
		meta := value.(StreamMeta)
		// Check if context is cancelled
		if r.ctx.Err() != nil {
			r.logger.Info(r.ctx, "Skipping CleanupOnShutdown due to context cancellation", map[string]interface{}{"stream": stream, "meta": meta})
			return false
		}
		ctx := r.ctx
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, r.writeTimeout)
			defer cancel()
		}
		if meta.ToDelete {
			if err := r.client.Del(ctx, stream).Err(); err == nil {
				r.logger.Info(ctx, "🔪 Deleted stream", map[string]interface{}{"stream": stream})
			}
		} else if meta.ToCleanGroup && meta.Group != "" {
			if err := r.client.XGroupDestroy(ctx, stream, meta.Group).Err(); err == nil {
				r.logger.Info(ctx, "🧹 Deleted consumer group", map[string]interface{}{
					"stream": stream,
					"group":  meta.Group,
				})
			} else {
				r.logger.Warn(ctx, "Failed to delete consumer group", map[string]interface{}{
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

func (r *RedisTransport) readGroup(ctx context.Context, stream, groupName, consumerName string, checkDeadline bool) ([]*api.TransportMessage, error) {
	r.updateStreamSubscriptionStatus(stream, true)
	//defer r.updateStreamSubscriptionStatus(stream, false)
	res, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{stream, ">"},
		Count:    r.streamReadCount,
		Block:    r.blockTimeout,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // ⬅️ No message, try again
		}
		r.logger.Error(ctx, "💥 Error reading (XReadGroup) from stream", map[string]interface{}{"stream": stream, "error": err.Error()})
		return nil, err
	}

	var messages []*api.TransportMessage
	for _, s := range res {
		for _, msg := range s.Messages {
			bodyRaw, ok := msg.Values["body"]
			if !ok {
				r.logger.Error(ctx, "⚠️ Missing 'body' field in message", map[string]interface{}{
					"stream": stream,
					"id":     msg.ID,
				})
				_ = r.client.XAck(ctx, s.Stream, groupName, msg.ID)
				continue
			}

			bodyStr, ok := bodyRaw.(string)
			if !ok {
				r.logger.Error(ctx, "⚠️ 'body' field is not a string", map[string]interface{}{
					"stream": stream,
					"id":     msg.ID,
				})
				_ = r.client.XAck(ctx, s.Stream, groupName, msg.ID)
				continue
			}

			processed, err := r.processData([]byte(bodyStr))
			if err != nil {
				r.logger.Error(ctx, "❌ Error processing message data", map[string]interface{}{
					"stream": stream,
					"id":     msg.ID,
					"error":  err.Error(),
				})
				_ = r.client.XAck(ctx, s.Stream, groupName, msg.ID)
				continue
			}

			decoded, err := api.DecodeTransportMessage(processed, msg.ID)
			if err != nil {
				r.logger.Error(ctx, "❌ Failed to decode TransportMessage", map[string]interface{}{
					"stream": stream,
					"id":     msg.ID,
					"error":  err.Error(),
				})
				_ = r.client.XAck(ctx, s.Stream, groupName, msg.ID)
				continue
			}

			messages = append(messages, decoded)
			// Optionally: store original Redis metadata in the TransportMessage if needed
		}
	}

	return messages, nil
}

// Request sends an RPC request and waits for a response.
func (r *RedisTransport) Request(ctx context.Context, stream string, msg *api.TransportMessage, timeout time.Duration, caller string) (*api.TransportMessage, error) {
	// ensure requested method and service exists
	exists, err := r.client.Exists(ctx, stream).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to verify stream(%s) existence: %w", stream, err)
	}
	if exists == 0 {
		r.logger.Error(r.ctx, "Requesting RPC stream that does not exists", map[string]interface{}{"stream": stream, "message": msg})
		return nil, fmt.Errorf("target RPC stream does not exist: %s", stream)
	}
	// Ensure ReplyTo is set
	if msg.ReplyTo == "" {
		if r.responseOnServiceStream {
			msg.ReplyTo = r.serviceReplyStream
		} else {
			msg.ReplyTo = fmt.Sprintf("reply:%s:%s:%s", stream, r.serviceInstanceID, caller)
		}
	}

	// Prepare response channel
	responseCh := make(chan *api.TransportMessage, 1)
	r.replyRouter.Store(msg.MessageID, responseCh)

	// Start listener goroutine
	//r.subscribedToServiceStream.Do(func() {
	//	r.supervisor.Go("redis_listenForReply", func(ctx context.Context) error {
	r.listenForReply(r.ctx, msg.ReplyTo, timeout)
	//		return nil
	//	})
	//})

	if err := r.Emit(ctx, stream, msg); err != nil {
		r.logger.Error(ctx, "Failed to send request after retries", map[string]interface{}{"error": err.Error()})
		return nil, fmt.Errorf("failed to send request after retries: %w", err)

	}
	// Encode message
	//msgBytes, err := msg.ToJson()
	//if err != nil {
	//	r.logger.Error("failed to encode TransportMessage", map[string]interface{}{"error": err.Error()})
	//	return nil, err
	//}

	//// Encrypt/compress if enabled
	//encodedData, err := r.prepareData(msgBytes)
	//if err != nil {
	//	r.logger.Error("prepareData error in Request", map[string]interface{}{"error": err.Error()})
	//	return nil, err
	//}

	//// Send to Redis stream
	//r.logger.Info("Sending RPC request", map[string]interface{}{
	//	"stream": stream,
	//	"size":   len(encodedData),
	//})
	//addOp := func() error {
	//	return r.client.XAdd(ctx, &redis.XAddArgs{
	//		Stream: stream,
	//		Values: map[string]interface{}{
	//			"body": encodedData,
	//		},
	//	}).Err()
	//}

	//if err := Retry(ctx, r.maxRetries, r.retryDelay, addOp); err != nil {
	//	r.logger.Error("Failed to send request after retries", map[string]interface{}{"error": err.Error()})
	//	return nil, fmt.Errorf("failed to send request after retries: %w", err)
	//}

	// Wait for reply
	select {
	case response := <-responseCh:
		return response, nil
	case <-time.After(timeout):
		r.replyRouter.Delete(msg.MessageID)
		return nil, fmt.Errorf("request timeout")
	}
}

func (r *RedisTransport) listenForReply(ctx context.Context, replyStream string, timeout time.Duration) {
	const groupName = "rpc_cg"
	//consumerName := uuid.New().String()
	consumerName := r.serviceInstanceID
	// Ensure consumer group exists (MKSTREAM allows stream autocreation)
	if err := r.ensureConsumerGroup(groupName, replyStream, StreamMeta{ToDelete: true, ToTrim: true, ToCleanPending: true, ToCleanGroup: true}); err != nil {
		r.logger.Error(ctx, "Failed to create consumer group", map[string]interface{}{
			"stream": replyStream,
			"group":  groupName,
			"error":  err.Error(),
		})
		return
	}
	if r.isSubscribedToStream(replyStream) {
		//r.logger.Warn("💥 Trying to read from an already subscribed to stream", map[string]interface{}{"stream": replyStream, "group": groupName, "consumer": consumerName})
		return
	}
	r.supervisor.GoLoop("redis_listenReply_"+replyStream, func(ctx context.Context) (time.Duration, error) {
		//r.updateStreamSubscriptionStatus(replyStream, true)
		//defer r.updateStreamSubscriptionStatus(stream, false)
		//for {
		//	select {
		//	case <-r.ctx.Done():
		//		r.logger.Info("listenForReply: context canceled", nil)
		//		return
		//	default:
		messages, err := r.readGroup(ctx, replyStream, groupName, consumerName, false)
		if err != nil {
			if r.ctx.Err() != nil {
				return 0, nil
			}
			r.logger.Error(ctx, "Error reading reply (XREADGROUP)", map[string]interface{}{"error": err.Error()})
			return 0, err
			//continue
		}
		for _, decoded := range messages {

			if decoded.Deadline > 0 && time.Now().After(time.Unix(0, decoded.Deadline)) {
				r.logger.Warn(ctx, "RPC response past deadline, discarding", map[string]interface{}{
					"messageID": decoded.MessageID,
					"replyTo":   replyStream,
				})
				if r.redisCfg.MoveExpiredToDLQ {
					//_ = PublishToDLQ(r.ctx, r.client, r.dlqStream, replyStream, body)
				}
				r.client.XAck(ctx, replyStream, groupName, decoded.ID)
				//continue
				return 0, fmt.Errorf("Past DEADLINE %s", decoded.MessageID)
			}

			// ✅ Send reply to waiting channel
			if ch, ok := r.replyRouter.LoadAndDelete(decoded.MessageID); ok {
				if typedCh, ok := ch.(chan *api.TransportMessage); ok {
					typedCh <- decoded
					// ✅ Acknowledge the message
					if err := r.client.XAck(ctx, replyStream, groupName, decoded.TransportID).Err(); err != nil {
						r.logger.Warn(ctx, "Failed to acknowledge reply message", map[string]interface{}{
							"stream":     replyStream,
							"group":      groupName,
							"message_id": decoded.TransportID,
							"error":      err.Error(),
						})
					}
				}
			} else {
				r.logger.Warn(ctx, "🔍 Stray RPC response unknown MessageID", map[string]interface{}{
					"messageID": decoded.MessageID,
					"replyTo":   replyStream,
				})
			}
		}
		return 0, nil
	})

	for !r.isSubscribedToStream(replyStream) {
		time.Sleep(10 * time.Millisecond) // or runtime.Gosched()
	}
	return
}

func (r *RedisTransport) cleanupExpiredPendingMessages(ctx context.Context, stream, group string) error {
	pendingRes, err := r.client.XPending(ctx, stream, group).Result()
	if err != nil {
		r.logger.Warn(ctx, "Failed XPENDING", map[string]interface{}{"stream": stream, "error": err.Error()})
		return nil
	}
	if pendingRes.Count == 0 {
		return nil
	}

	entries, err := r.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		r.logger.Warn(ctx, "Failed XPendingExt", map[string]interface{}{"stream": stream, "error": err.Error()})
		return nil
	}

	for _, entry := range entries {
		msgID := entry.ID
		msgs, err := r.client.XRange(ctx, stream, msgID, msgID).Result()
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
		decoded, err := api.DecodeTransportMessage(processed, msgs[0].ID)
		if err != nil {
			continue
		}
		if decoded.Deadline > 0 && time.Now().After(time.Unix(0, decoded.Deadline)) {
			r.logger.Warn(ctx, "Removing expired pending message", map[string]interface{}{
				"stream": stream,
				"msgID":  msgID,
			})
			r.client.XAck(ctx, stream, group, msgID)
			if r.redisCfg.MoveExpiredToDLQ {
				_ = PublishToDLQ(r.ctx, r.client, r.dlqStream, stream, bodyStr)
			}
		}
	}
	return nil
}

func (r *RedisTransport) Publish(ctx context.Context, stream string, data []byte) error {
	r.registerStream(stream, StreamMeta{
		//ToTrim: r.redisCfg.StreamTrimMaxLen > 0,
		ToTrim: true,
	})
	if r.isStreamOverflowing(stream) {

		r.logger.Warn(ctx, "stream is currently overflowing, throttling in place", map[string]interface{}{
			"stream": stream,
		})
		time.Sleep(1000 * time.Millisecond)
		//return fmt.Errorf("", stream)
	}
	addOp := func() error {
		_, err := r.client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"body": data},
		}).Result()
		return err
	}
	if err := Retry(ctx, r.maxRetries, r.retryDelay, addOp); err != nil {
		r.logger.Error(ctx, "Failed to publish message after retries", map[string]interface{}{"error": err.Error()})
		if dlqErr := PublishToDLQ(ctx, r.client, r.dlqStream, stream, data); dlqErr != nil {
			r.logger.Error(ctx, "Failed to publish to DLQ", map[string]interface{}{"error": dlqErr.Error()})
		}
		return fmt.Errorf("failed to publish message after retries: %w", err)
	}

	r.logger.Debug(ctx, "Publishing message", map[string]interface{}{
		"stream": stream,
		"size":   len(data),
	})

	return nil
}

func (r *RedisTransport) Emit(ctx context.Context, stream string, msg *api.TransportMessage) error {
	//start := time.Now()
	jsonMsg, err := msg.ToJson()
	if err != nil {
		r.logger.Error(ctx, "Emit: failed to encode message", map[string]interface{}{
			"stream": stream,
			"error":  err.Error(),
			"msg":    msg,
		})
		r.metrics.IncrementTagged("transport_errors", "stream", stream, "stage", "emit_msg_tojson")
		return err
	}
	encodedMsg, err := r.prepareData(jsonMsg)
	if err != nil {
		r.logger.Error(ctx, "prepareData Message to Publish Error", map[string]interface{}{
			"stream": stream,
			"error":  err.Error(),
			"msg":    jsonMsg,
		})
		return err
	}
	return r.Publish(ctx, stream, encodedMsg)
}

func (r *RedisTransport) Subscribe(ctx context.Context, consumerGroup string, stream string, handler api.HandlerFunc, checkDeadline bool) error {
	group := "consumer_group"
	if consumerGroup != "" {
		group = consumerGroup
	}
	//consumer := uuid.New().String()
	consumer := r.serviceInstanceID

	if err := r.ensureConsumerGroup(group, stream, StreamMeta{ToDelete: false, ToTrim: true, ToCleanPending: true, ToCleanGroup: false}); err != nil {
		r.logger.Error(ctx, "Failed to create consumer group", map[string]interface{}{
			"stream": stream,
			"group":  group,
			"error":  err.Error(),
		})
		return err
	}

	r.logger.Info(ctx, "Subscribing to stream", map[string]interface{}{
		"stream": stream,
	})
	if r.isSubscribedToStream(stream) {
		r.logger.Warn(ctx, "💥 Trying to read from an already subscribed to stream", map[string]interface{}{"stream": stream, "group": group, "consumer": consumer})
		return fmt.Errorf("Trying to read from an already subscribed to stream %s", stream)
	}

	r.supervisor.GoLoop("redis_subscribe_"+stream, func(ctx context.Context) (time.Duration, error) {

		r.updateStreamSubscriptionStatus(stream, true)
		messages, err := r.readGroup(ctx, stream, group, consumer, checkDeadline)
		if err != nil || messages == nil {
			return 0, nil // ⬅️ No message, try again
		}

		for _, decoded := range messages {

			addOp := func() error {
				_, handlerErr := handler(ctx, decoded)
				return handlerErr
			}
			if err := Retry(ctx, r.maxRetries, r.retryDelay, addOp); err != nil {
				r.logger.Error(ctx, "Handler failed after retries; sending to DLQ", map[string]interface{}{"msgID": decoded.ID, "error": err.Error()})
				if dlqErr := PublishToDLQ(ctx, r.client, r.dlqStream, stream, decoded); dlqErr != nil {
					r.logger.Error(ctx, "Failed to publish to DLQ", map[string]interface{}{"error": dlqErr.Error()})
				}
				r.client.XAck(ctx, stream, group, decoded.ID)
				return 0, fmt.Errorf("failed to handle received message after retries: %w", err)
			}
			r.client.XAck(ctx, stream, group, decoded.ID)
			return 0, nil

			//var handlerErr error
			//for attempt := 1; attempt <= r.maxRetries; attempt++ {
			//	_, handlerErr = handler(decoded)
			//	if handlerErr == nil {
			//		break
			//	}
			//	r.logger.Error("Handler error", map[string]interface{}{
			//		"attempt": attempt,
			//		"msgID":   decoded.ID,
			//		"error":   handlerErr.Error(),
			//	})
			//	time.Sleep(r.retryDelay)
			//	return 0, handlerErr
			//}
			//if handlerErr == nil {
			//	r.client.XAck(ctx, stream, group, decoded.ID)
			//} else {
			//	r.logger.Error("Handler failed after retries; sending to DLQ", map[string]interface{}{"msgID": decoded.ID})
			//	//if dlqErr := PublishToDLQ(r.ctx, r.client, r.dlqStream, stream, body); dlqErr != nil {
			//	//	r.logger.Error("Failed to publish to DLQ", map[string]interface{}{"error": dlqErr.Error()})
			//	//}
			//	r.client.XAck(ctx, stream, group, decoded.ID)
			//}
		}
		return 0, nil
	})

	for !r.isSubscribedToStream(stream) {
		time.Sleep(10 * time.Millisecond)
	}
	return nil
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
