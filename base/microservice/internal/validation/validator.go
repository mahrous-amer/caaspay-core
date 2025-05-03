package validation

import (
	"fmt"
	"github.com/go-playground/validator/v10"
)

// Validator wraps the go-playground validator for structured validation.
type Validator struct {
	validate *validator.Validate
}

// NewValidator creates a new Validator instance.
func NewValidator() *Validator {
	v := validator.New()
	return &Validator{validate: v}
}

// ValidateStruct validates the given struct and returns an error if invalid.
func (v *Validator) ValidateStruct(s interface{}) error {
	if err := v.validate.Struct(s); err != nil {
		if validationErrors, ok := err.(validator.ValidationErrors); ok {
			errors := make(map[string]string)
			for _, e := range validationErrors {
				errors[e.Field()] = fmt.Sprintf("%s validation failed on '%s'", e.Tag(), e.Param())
			}
			return fmt.Errorf("validation errors: %v", errors)
		}
		return err
	}
	return nil
}
