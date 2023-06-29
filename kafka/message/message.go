package message

import "github.com/segmentio/kafka-go"

type Message interface {
	ToKafkaMessage() kafka.Message
	GetValueForDump() string
}

type SimpleMessage struct {
	Value string
}

func (s *SimpleMessage) ToKafkaMessage() kafka.Message {
	return kafka.Message{Value: []byte(s.Value)}
}

func (s *SimpleMessage) GetValueForDump() string {
	if s == nil {
		return ""
	}
	return s.Value
}
