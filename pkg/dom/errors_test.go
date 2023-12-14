package dom

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestErrRateLimitExceeded_Error(t *testing.T) {
	r := require.New(t)
	sourceErr := &ErrRateLimitExceeded{RetryIn: time.Second}
	errWrapped := fmt.Errorf("%w: wrapped", sourceErr)
	otherErr := ErrNotFound
	var as *ErrRateLimitExceeded

	var err error = sourceErr
	r.ErrorAs(err, &as)
	r.EqualValues(sourceErr.RetryIn, as.RetryIn)

	r.ErrorAs(errWrapped, &as)
	r.EqualValues(sourceErr.RetryIn, as.RetryIn)

	if errors.As(otherErr, &as) {
		t.Error("wrong error casted")
	}
}
