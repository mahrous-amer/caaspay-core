package transport

import (
	"time"
)

// Retry executes the given operation, retrying it up to 'attempts' times with exponential backoff.
func Retry(attempts int, delay time.Duration, operation func() error) error {
	err := operation()
	if err == nil {
		return nil
	}
	for i := 1; i < attempts; i++ {
		time.Sleep(delay)
		err = operation()
		if err == nil {
			return nil
		}
		delay *= 2 // Exponential backoff
	}
	return err
}
