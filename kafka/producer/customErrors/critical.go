package customErrors

import (
	"fmt"
	"time"
)

type CriticalError struct {
	msg string
}

func (e CriticalError) Error() string {
	return e.msg
}

func NewCriticalError(err error, thread int) CriticalError {
	return CriticalError{msg: fmt.Sprintf("kafka unreachalbe at %v, error %s, threadId %v", time.Now(), err.Error(), thread)}
}
