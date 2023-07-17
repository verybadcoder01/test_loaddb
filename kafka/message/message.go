package message

import (
	"time"

	"github.com/segmentio/kafka-go"
)

type Message interface {
	ToKafkaMessage() kafka.Message
	GetValueForDump() string
	GetTimestamp() time.Time // for non-timestamped messages just leave blank
}

type TimestampedMessage struct {
	TimeStamp time.Time // required for soring by time
	Value     string
}

func (s *TimestampedMessage) ToKafkaMessage() kafka.Message {
	return kafka.Message{Value: []byte(s.Value)}
}

func (s *TimestampedMessage) GetValueForDump() string {
	if s == nil {
		return ""
	}
	return s.Value
}

func (s *TimestampedMessage) GetTimestamp() time.Time {
	return s.TimeStamp
}
