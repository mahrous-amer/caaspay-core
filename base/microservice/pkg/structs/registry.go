package structs

import "reflect"

// Global registry of request/response struct types used across API and services
var TypeRegistry = map[string]reflect.Type{
	"example.PingRequest":  reflect.TypeOf(PingRequest{}),
	"example.PingResponse": reflect.TypeOf(PingResponse{}),
}
