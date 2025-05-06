package api

// HandlerFunc defines a callback for processing incoming messages.
type HandlerFunc func(request []byte) ([]byte, error)
