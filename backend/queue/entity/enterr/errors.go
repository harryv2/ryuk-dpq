package enterr

import (
	"errors"
	"fmt"
)

// Code is what a controller maps to a status.
type Code string

const (
	CodeInvalid   Code = "invalid"
	CodeNotFound  Code = "not_found"
	CodeConflict  Code = "conflict"
	CodeExhausted Code = "exhausted"
	CodeMoved     Code = "moved"
	CodeInternal  Code = "internal"
)

type Error struct {
	Code    Code
	Message string
	Detail  string
	err     error
}

func (e *Error) Error() string {
	if e.Detail != "" {
		return string(e.Code) + ": " + e.Message + ": " + e.Detail
	}
	return string(e.Code) + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.err }

func New(code Code, msg string) *Error { return &Error{Code: code, Message: msg} }

func Wrap(code Code, msg string, err error) *Error {
	return &Error{Code: code, Message: msg, Detail: err.Error(), err: err}
}

func Invalid(format string, a ...any) *Error {
	return New(CodeInvalid, fmt.Sprintf(format, a...))
}

func NotFound(what string) *Error {
	return New(CodeNotFound, what+" not found")
}

func Internal(msg string, err error) *Error { return Wrap(CodeInternal, msg, err) }

// Moved carries the node that now owns the queue so the caller can retry.
func Moved(owner string) *Error {
	return &Error{Code: CodeMoved, Message: "queue moved", Detail: owner}
}

func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeInternal
}
