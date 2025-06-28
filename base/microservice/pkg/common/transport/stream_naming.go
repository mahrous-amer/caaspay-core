package transport

import (
	"fmt"
	"strings"
)

// StreamType represents the kind of stream: RPC, emitter, receiver.
type StreamType string

const (
	StreamTypeRPC      StreamType = "rpc"
	StreamTypeEmitter  StreamType = "emitter"
	StreamTypeReceiver StreamType = "receiver"
	StreamTypeReply    StreamType = "reply"
	StreamTypeService  StreamType = "service"
)

// StreamConfig defines a consistent stream naming structure.
type StreamConfig struct {
	Type       StreamType // "rpc", "emitter", "receiver"
	Service    string     // e.g., fxrate
	Method     string     // e.g., UpdateRates
	InstanceID string     // optional, e.g., instance-01
}

func (s *StreamConfig) Normalize() {
	s.Type = StreamType(strings.ToLower(string(s.Type)))
	s.Service = strings.ToLower(s.Service)
	s.Method = strings.ToLower(s.Method)
	s.InstanceID = strings.ToLower(s.InstanceID)
}

func (s *StreamConfig) Validate() error {
	if s.Type == "" {
		return fmt.Errorf("stream type is required")
	}

	if s.Service == "" {
		return fmt.Errorf("service name is required")
	}

	switch s.Type {
	case StreamTypeRPC, StreamTypeEmitter, StreamTypeReceiver:
		if s.Method == "" {
			return fmt.Errorf("method is required for stream type '%s'", s.Type)
		}
	case StreamTypeReply, StreamTypeService:
		// Method can be empty
	default:
		return fmt.Errorf("invalid stream type '%s'", s.Type)
	}

	// Optionally: ensure no colons in fields
	if strings.ContainsAny(s.Service, ":") || strings.ContainsAny(s.Method, ":") || strings.ContainsAny(s.InstanceID, ":") {
		return fmt.Errorf("service, method, and instanceID must not contain ':'")
	}

	return nil
}

// BuildStreamName generates a canonical stream name:
// Format: <type>:<service>:<method>[:<instanceID>]
func BuildStreamName(cfg StreamConfig) string {
	cfg.Normalize()
	base := fmt.Sprintf("%s:%s", cfg.Service, cfg.Method)
	if cfg.InstanceID != "" {
		return fmt.Sprintf("%s:%s:%s", cfg.Type, base, cfg.InstanceID)
	}
	return fmt.Sprintf("%s:%s", cfg.Type, base)
}

// ParseStreamName converts a stream name string into a StreamConfig.
// Expected format: <type>:<service>:<method>[:<instanceID>]
func ParseStreamName(stream string) (*StreamConfig, error) {
	parts := strings.Split(stream, ":")

	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid stream name format: %s", stream)
	}

	cfg := &StreamConfig{
		Type:    StreamType(parts[0]),
		Service: parts[1],
		Method:  parts[2],
	}

	if len(parts) > 3 {
		cfg.InstanceID = parts[3]
	}

	cfg.Normalize()
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
