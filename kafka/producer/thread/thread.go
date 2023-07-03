package thread

import (
	"dbload/kafka/message"
	"dbload/kafka/producer/buffer"
	log "github.com/sirupsen/logrus"
)

type Status int

// Finished is very convenient to make 0, because reading from closed channel gives us 0, and we only read from closed channel after thread is finished
const (
	FINISHED Status = iota
	OK
	DEAD
)

type Thread struct {
	isDone     bool
	StatusChan chan Status
	msgBuffer  buffer.MessageBuffer
}

func NewThread(statusChan chan Status, buffer buffer.MessageBuffer) Thread {
	return Thread{isDone: false, StatusChan: statusChan, msgBuffer: buffer}
}

func (t *Thread) AppendBuffer(logger *log.Logger, msg ...message.Message) {
	t.msgBuffer.Append(logger, msg...)
}

func (t *Thread) DumpBuffer(logger *log.Logger) {
	t.msgBuffer.Dump(logger)
}

func (t *Thread) FinishThread(logger *log.Logger) {
	t.isDone = true
	t.DumpBuffer(logger)
}

func (t *Thread) ExtractBatchFromBuffer(batchSz int) []message.Message {
	return t.msgBuffer.ExtractBatch(batchSz)
}
