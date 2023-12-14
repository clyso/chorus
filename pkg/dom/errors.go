package dom

import (
	"errors"
	"net/http"
	"time"
)

var (
	ErrInternal             = errors.New("InternalError")
	ErrNotImplemented       = errors.New("NotImplemented")
	ErrInvalidStorageConfig = errors.New("InvalidStorageConfig")
	ErrAlreadyExists        = errors.New("AlreadyExists")
	ErrNotFound             = errors.New("NotFound")
	ErrInvalidArg           = errors.New("InvalidArg")
	ErrAuth                 = errors.New("AuthError")
	ErrPolicy               = errors.New("PolicyError")
)

func ErrCode(err error) int {
	switch {
	case errors.Is(err, ErrNotImplemented):
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

type ErrRateLimitExceeded struct {
	RetryIn time.Duration
}

func (e *ErrRateLimitExceeded) Error() string {
	return "RateLimitExceeded"
}
