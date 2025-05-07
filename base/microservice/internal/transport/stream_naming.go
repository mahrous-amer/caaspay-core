package transport

import (
	"fmt"
	"strings"

	"github.com/caaspay/caaspay-core/pkg/api"
)

// BuildStreamName generates a canonical stream name:
// Format: <type>:<service>:<method>[:<instanceID>]
func BuildStreamName(cfg api.StreamConfig) string {
	base := fmt.Sprintf("%s:%s", cfg.Service, cfg.Method)
	if cfg.InstanceID != "" {
		return fmt.Sprintf("%s:%s:%s", cfg.Type, base, cfg.InstanceID)
	}
	return fmt.Sprintf("%s:%s", cfg.Type, base)
}

// ParseStreamName converts a stream name string into a StreamConfig.
// Expected format: <type>:<service>:<method>[:<instanceID>]
func ParseStreamName(stream string) (*api.StreamConfig, error) {
	parts := strings.Split(stream, ":")

	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid stream name format: %s", stream)
	}

	cfg := &api.StreamConfig{
		Type:    api.StreamType(parts[0]),
		Service: parts[1],
		Method:  parts[2],
	}

	if len(parts) > 3 {
		cfg.InstanceID = parts[3]
	}

	return cfg, nil
}

// Example usage:
// BuildStreamName(StreamConfig{
//   Type: StreamTypeRPC,
//   Service: "fxrate",
//   Method: "GetRates",
//   InstanceID: "instance-01",
// })
// Output: "rpc:fxrate:GetRates:instance-01"
