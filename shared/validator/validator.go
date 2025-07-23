// Package validator provides utilities for validating and decoding JSON input
// into strongly typed structs, using the go-playground/validator package.
package validator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"reflect"

	"github.com/gorilla/schema"

	"github.com/go-playground/validator"
)

// schemaDecoder is a shared decoder configured to ignore unknown fields.
var (
	schemaDecoder *schema.Decoder
	validate      *validator.Validate
)

func init() {
	validate = validator.New()
	schemaDecoder = schema.NewDecoder()
	schemaDecoder.IgnoreUnknownKeys(true)
	// Register FlexibleTime type with the decoder
	schemaDecoder.RegisterConverter(FlexibleTime{}, func(s string) reflect.Value {
		var ft FlexibleTime
		if err := ft.UnmarshalText([]byte(s)); err != nil {
			return reflect.Value{}
		}
		return reflect.ValueOf(ft)
	})
}

// Validate decodes JSON data from the provided io.Reader into a struct of type T,
// and validates the struct using the go-playground/validator package.
//
// It returns the decoded struct if successful, or an error if either the JSON is
// malformed or the struct fails validation.
//
// Example usage:
//
//	type User struct {
//	    Name  string `json:"name" validate:"required,min=3"`
//	    Email string `json:"email" validate:"required,email"`
//	}
//
//	user, err := validator.Validate\[User\](reader)
//	if err != nil {
//	    // Handle error
//	}
//	// Use 'user' which is the decoded and validated struct
//
// Parameters:
//   - r: an io.Reader that contains the JSON data to be decoded and validated.
//
// Returns:
//   - T: The decoded and validated struct of type T.
//   - error: If the JSON is invalid or validation fails, an error is returned.
func Validate[T any](r io.Reader) (T, error) {
	var res T
	var zero T
	if err := json.NewDecoder(r).Decode(&res); err != nil {
		return zero, fmt.Errorf("invalid JSON: %w", err)
	}
	if err := validate.Struct(res); err != nil {
		return zero, fmt.Errorf("validation failed: %w", err)
	}
	return res, nil
}

// ValidateBody decodes JSON from the request body into a struct of type T,
// validates the struct using the go-playground/validator package, and resets the request body.
//
// It returns the decoded struct if successful, or an error if either the JSON is
// malformed or validation fails.
func ValidateBody[T any](r *http.Request) (T, error) {
	var zero T
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return zero, err
	}
	// Reset the request body for subsequent reads
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	return Validate[T](bytes.NewReader(bodyBytes))
}

// ValidateQuery decodes URL query parameters into a struct of type T
// and validates it using go-playground/validator.
//
// Example usage:
//
//	type Filters struct {
//	    Limit int    `schema:"limit" validate:"gte=1,lte=100"`
//	    Sort  string `schema:"sort" validate:"oneof=asc desc"`
//	}
//
//	params := r.URL.Query()
//	filters, err := validator.ValidateQuery\[Filters\](params)
//
// Parameters:
//   - q: url.Values from r.URL.Query()
//
// Returns:
//   - T: Decoded and validated struct.
//   - error: If decoding or validation fails.
func ValidateQuery[T any](q url.Values) (T, error) {
	var res T
	var empty T
	if err := schemaDecoder.Decode(&res, q); err != nil {
		return empty, fmt.Errorf("invalid query params: %w", err)
	}
	// Specific to any FlexibleTime fields, applies defaults if necessary
	applyFlexibleTimeDefaults(&res)
	if err := validate.Struct(res); err != nil {
		return empty, fmt.Errorf("validation failed: %w", err)
	}
	return res, nil
}

// ValidateRequest parses and validates HTTP request parameters into a struct of type T.
//
// This generic function is designed to work with both GET and POST requests. It uses `r.ParseForm()`
// to populate `r.Form` with all query parameters and (for POST, PUT, PATCH requests with the correct
// content type) form body parameters. For overlapping keys, POST body parameters take precedence.
//
// The function then calls ValidateQuery to decode the merged parameters into a struct of type T and
// validates it using go-playground/validator, returning either the decoded struct or an error.
//
// Returns:
//   - T: The decoded and validated struct of type T.
//   - error: An error if decoding or validation fails.
//
// Example:
//
//	type UserParams struct {
//	    Name  string `schema:"name" validate:"required"`
//	    Email string `schema:"email" validate:"required,email"`
//	}
//
//	user, err := ValidateRequest[UserParams](r)
//	if err != nil {
//	    // handle error
//	}
func ValidateRequest[T any](r *http.Request) (T, error) {
	if err := r.ParseForm(); err != nil {
		var empty T
		return empty, fmt.Errorf("in ValidateRequest: failed to parse form data: %w", err)
	}
	// ParseForm prioritizes PostForm over URL query parameters,
	// however the schemaDecoder prioritizes the last slice value.
	// see TestDecoderFormPrecedence
	values := r.Form
	// Overriding the keys with PostForm values
	maps.Copy(values, r.PostForm)
	return ValidateQuery[T](r.Form)
}
