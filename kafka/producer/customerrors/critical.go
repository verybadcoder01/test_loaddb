package customerrors

import (
	"fmt"
)

// CriticalError are errors that require os.Exit(1) (aka log.Fatal) or panic() to be called
type CriticalError struct {
	msg string
}

func (e *CriticalError) Error() string {
	return e.msg
}

func NewCriticalError(err error) CriticalError {
	return CriticalError{msg: err.Error()}
}

func (e *CriticalError) Wrap(params map[string]interface{}) {
	for k, v := range params {
		e.msg += fmt.Sprintf(" %s=%v ", k, v)
	}
}
