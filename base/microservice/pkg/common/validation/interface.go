package validation

// --- Validator abstraction ---
type ValidatorInterface interface {
	ValidateStruct(input interface{}) error
}
