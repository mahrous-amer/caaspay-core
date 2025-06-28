package fwctx

type ContextKey string

const (
	CtxKeyMessageID ContextKey = "msg_transport_id"
	CtxKeyStream    ContextKey = "stream"
	CtxKeyRStream   ContextKey = "reply_stream"
	CtxKeyTraceID   ContextKey = "trace_id"
	CtxKeySpanID    ContextKey = "span_id"

	CtxKeyUserID ContextKey = "user_id"
	CtxKeyLocale ContextKey = "locale"
	CtxKeyIP     ContextKey = "ip"
	CtxKeySource ContextKey = "source"
)
