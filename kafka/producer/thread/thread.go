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
	StatusChan chan Status
	SorterChan chan []message.Message
	msgBuffer  buffer.MessageBuffer
}

func NewThread(statusChan chan Status, sorterChan chan []message.Message, buffer buffer.MessageBuffer) Thread {
	return Thread{StatusChan: statusChan, msgBuffer: buffer, SorterChan: sorterChan}
}

func (t *Thread) AppendBuffer(logger *log.Logger, msg ...message.Message) {
	t.msgBuffer.Append(logger, msg...)
}

func (t *Thread) DumpBuffer(logger *log.Logger) {
	t.msgBuffer.Dump(logger)
}

func (t *Thread) FinishThread(logger *log.Logger) {
	t.StatusChan <- FINISHED
	close(t.StatusChan)
	close(t.SorterChan)
	t.DumpBuffer(logger)
}

func (t *Thread) ExtractBatchFromBuffer(batchSz int) []message.Message {
	return t.msgBuffer.ExtractBatch(batchSz)
}
