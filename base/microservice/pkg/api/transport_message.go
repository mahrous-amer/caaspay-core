package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// TransportMessage is the core message structure for all Redis-based communications.
type TransportMessage struct {
	MessageID   string            `json:"message_id"`
	TransportID string            `json:"transport_id"`
	Service     string            `json:"service"`
	Method      string            `json:"method"`
	Who         string            `json:"who,omitempty"`
	ReplyTo     string            `json:"reply_to,omitempty"`
	Deadline    int64             `json:"deadline"`
	Auth        *AuthContext      `json:"auth,omitempty"`
	Context     *RequestContext   `json:"context,omitempty"`
	Args        json.RawMessage   `json:"args,omitempty"`
	Response    json.RawMessage   `json:"response,omitempty"`
	Stash       map[string]any    `json:"stash,omitempty"`
	Trace       map[string]string `json:"trace,omitempty"`
	Error       *MessageError     `json:"error,omitempty"`
}

// AuthContext holds authentication metadata.
type AuthContext struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Role     string `json:"role"`
	Token    string `json:"token,omitempty"`
}

// RequestContext holds request metadata (IP, locale, etc).
type RequestContext struct {
	IP        string `json:"ip,omitempty"`
	Locale    string `json:"locale,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	Source    string `json:"source,omitempty"`
}

// MessageError describes an error response.
type MessageError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Retry   bool   `json:"retry,omitempty"`
}

// NewTransportMessage creates a new message with defaults.
func NewTransportMessage(service, method string, rawArgs json.RawMessage) *TransportMessage {
	return &TransportMessage{
		MessageID:   uuid.NewString(),
		TransportID: uuid.NewString(),
		Service:     service,
		Method:      method,
		Deadline:    time.Now().Add(30 * time.Second).Unix(),
		Args:        rawArgs,
		Stash:       map[string]any{},
		Trace:       map[string]string{},
	}
}

// Validate checks if the required fields are present.
func (m *TransportMessage) Validate() error {
	if m.MessageID == "" || m.TransportID == "" {
		return errors.New("missing message_id or transport_id")
	}
	if m.Service == "" || m.Method == "" {
		return errors.New("missing service or method")
	}
	if m.Deadline <= 0 {
		return errors.New("invalid deadline")
	}
	return nil
}

// Marshal encodes the message to JSON.
func (m *TransportMessage) Encode() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalTransportMessage decodes the message from JSON.
func DecodeTransportMessage(data []byte) (*TransportMessage, error) {
	var msg TransportMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (m *TransportMessage) ValidateAgainst(schema any) error {
	if schema == nil {
		return nil
	}

	ptr := reflect.New(reflect.TypeOf(schema)).Interface()
	if err := json.Unmarshal(m.Args, &ptr); err != nil {
		return fmt.Errorf("invalid args: %w", err)
	}

	// Optional: Validate `ptr` if it has a `Validate()` method
	if validator, ok := ptr.(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return fmt.Errorf("args validation failed: %w", err)
		}
	}

	return nil
}
