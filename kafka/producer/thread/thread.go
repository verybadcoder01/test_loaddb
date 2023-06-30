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
	IsDone     bool
	StatusChan chan Status
	MsgBuffer  buffer.MessageBuffer
	MaxBufSize int
}

func (t *Thread) AppendBuffer(logger *log.Logger, msg ...message.Message) {
	t.MsgBuffer.Append(logger, msg...)
}

func (t *Thread) DumpBuffer(logger *log.Logger) {
	t.MsgBuffer.Dump(logger)
}

func (t *Thread) FinishThread(logger *log.Logger) {
	t.IsDone = true
	t.DumpBuffer(logger)
}

func (t *Thread) ExtractBatchFromBuffer(batchSz int) []message.Message {
	return t.MsgBuffer.ExtractBatch(batchSz)
}
