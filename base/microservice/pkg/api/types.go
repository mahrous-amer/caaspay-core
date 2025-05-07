package api

// HandlerFunc defines a callback for processing incoming messages.
type HandlerFunc func(request []byte) ([]byte, error)

// StreamType represents the kind of stream: RPC, emitter, receiver.
type StreamType string

const (
	StreamTypeRPC      StreamType = "rpc"
	StreamTypeEmitter  StreamType = "emitter"
	StreamTypeReceiver StreamType = "receiver"
)

// StreamConfig defines a consistent stream naming structure.
type StreamConfig struct {
	Type       StreamType // "rpc", "emitter", "receiver"
	Service    string     // e.g., fxrate
	Method     string     // e.g., UpdateRates
	InstanceID string     // optional, e.g., instance-01
}
