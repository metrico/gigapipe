package errors

import (
	"errors"
)

// Define base error values for comparison
var (
	ErrNotImplemented = &QrynError{501, "not implemented"}
	ErrNotFound       = &QrynError{404, "not implemented"}
)

// IQrynError interface
type IQrynError interface {
	error
	IsQrynError() bool
	GetCode() int
}

type UnMarshalError struct {
	Message string
	Code    int
}

func (u *UnMarshalError) GetCode() int {
	return u.Code
}

func (u *UnMarshalError) IsQrynError() bool {
	return true
}

func (u *UnMarshalError) Error() string {
	return u.Message
}

// QrynError struct implementing IQrynError
type QrynError struct {
	Code    int
	Message string
}

func (e *QrynError) Error() string {
	return e.Message
}

func (e *QrynError) IsQrynError() bool {
	return true
}

func (e *QrynError) GetCode() int {
	return e.Code
}

func New400Error(msg string) IQrynError {
	return &QrynError{Code: 400, Message: msg}
}

func New401Error(msg string) IQrynError {
	return &QrynError{Code: 401, Message: msg}
}

func New429Error(msg string) IQrynError {
	return &QrynError{Code: 429, Message: msg}
}

// NewUnmarshalError creates a new instance of UnmarshalError.
func NewUnmarshalError(err error) IQrynError {
	var target IQrynError
	if errors.As(err, &target) {
		return target
	}
	return &UnMarshalError{
		err.Error(),
		400,
	}
}

func Unwrap[T IQrynError](err error) (T, bool) {
	var target T
	if errors.As(err, &target) {
		return target, true
	}
	return target, false
}
