package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// TransportMessage is the core message structure for all Redis-based communications.
type TransportMessage struct {
	ID          string            `json:"id"`
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
func NewTransportMessage(service, method string, rawArgs json.RawMessage, timeout ...time.Duration) *TransportMessage {
	// Default to 30 seconds
	t := 30 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}

	return &TransportMessage{
		MessageID:   uuid.NewString(),
		TransportID: uuid.NewString(),
		Service:     service,
		Method:      method,
		Deadline:    time.Now().Add(t).UnixNano(),
		Args:        rawArgs,
		Stash:       map[string]any{},
		Trace:       map[string]string{},
	}
}

func NewHTTPMessage(method, url string, body []byte, headers map[string]string, timeout time.Duration) *TransportMessage {
	msg := NewTransportMessage("external_http", fmt.Sprintf("%s:%s", method, url), body, timeout)
	msg.Stash = map[string]any{
		"http_url":     url,
		"http_method":  method,
		"http_headers": headers,
	}
	return msg
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
func (m *TransportMessage) ToJson() ([]byte, error) {
	return json.Marshal(m)
}

// ToMap converts the TransportMessage to a map for broker use.
func (m *TransportMessage) ToMap() (map[string]interface{}, error) {
	var msgMap map[string]interface{}
	// A reliable way to convert a struct to a map is to marshal and unmarshal it.
	data, err := json.Marshal(m)
	if err!= nil {
		return nil, fmt.Errorf("failed to marshal message to map: %w", err)
	}
	err = json.Unmarshal(data, &msgMap)
	if err!= nil {
		return nil, fmt.Errorf("failed to unmarshal message to map: %w", err)
	}
	return msgMap, nil
}

// UnmarshalTransportMessage decodes the message from JSON.
func DecodeTransportMessage(data []byte, transportID string) (*TransportMessage, error) {
	var msg TransportMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	msg.TransportID = transportID
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

func (m *TransportMessage) AttachToContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, CtxKeyMessageID, m.TransportID)
	ctx = context.WithValue(ctx, CtxKeyStream, m.Method)
	ctx = context.WithValue(ctx, CtxKeyRStream, m.ReplyTo)

	// Auth metadata
	if m.Auth != nil && m.Auth.UserID != "" {
		ctx = context.WithValue(ctx, CtxKeyUserID, m.Auth.UserID)
	}

	// Request metadata
	if m.Context != nil {
		if m.Context.IP != "" {
			ctx = context.WithValue(ctx, CtxKeyIP, m.Context.IP)
		}
		if m.Context.Locale != "" {
			ctx = context.WithValue(ctx, CtxKeyLocale, m.Context.Locale)
		}
		if m.Context.Source != "" {
			ctx = context.WithValue(ctx, CtxKeySource, m.Context.Source)
		}
	}

	// Optional: add trace/span ID if you extract from m.Trace map
	if traceID, ok := m.Trace["trace_id"]; ok {
		ctx = context.WithValue(ctx, CtxKeyTraceID, traceID)
	}
	if spanID, ok := m.Trace["span_id"]; ok {
		ctx = context.WithValue(ctx, CtxKeySpanID, spanID)
	}

	return ctx
}
