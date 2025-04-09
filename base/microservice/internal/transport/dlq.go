package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// PublishToDLQ publishes a message to the DLQ for further inspection.
func PublishToDLQ(ctx context.Context, client *redis.Client, dlqStream string, originalStream string, message interface{}) error {
	dlqMessage := map[string]interface{}{
		"original_stream": originalStream,
		"body":            message,
		"timestamp":       time.Now().Unix(),
	}
	_, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqStream,
		Values: dlqMessage,
	}).Result()
	if err != nil {
		return fmt.Errorf("PublishToDLQ: error publishing to DLQ: %w", err)
	}
	return nil
}
