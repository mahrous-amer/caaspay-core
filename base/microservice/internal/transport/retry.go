package transport

import (
	"context"
	"time"
)

// Retry executes the given operation, retrying it up to 'attempts' times with exponential backoff.
// If the context is canceled, it stops and returns ctx.Err().
func Retry(ctx context.Context, attempts int, delay time.Duration, operation func() error) error {
	err := operation()
	if err == nil {
		return nil
	}

	for i := 1; i < attempts; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		err = operation()
		if err == nil {
			return nil
		}

		delay *= 2 // Exponential backoff
	}

	return err
}
