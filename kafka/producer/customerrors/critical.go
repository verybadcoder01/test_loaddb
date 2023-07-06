package customerrors

import (
	"fmt"
)

// CustomError are errors that can be wrapped with additional info
type CustomError struct {
	msg string
}

func (e *CustomError) Error() string {
	return e.msg
}

func NewCriticalError(err error) CustomError {
	return CustomError{msg: err.Error()}
}

func (e *CustomError) Wrap(params map[string]interface{}) {
	for k, v := range params {
		e.msg += fmt.Sprintf(" %s=%v ", k, v)
	}
}
