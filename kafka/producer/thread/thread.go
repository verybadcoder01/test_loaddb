package thread

import (
	"dbload/kafka/message"
	"dbload/kafka/producer/customErrors"
	"dbload/kafka/producer/dumper"
	"errors"
	"github.com/gammazero/deque"
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
	MsgBuffer  deque.Deque[message.Message] // TODO: I better extract this to buffer interface
	Dumper     dumper.Dumper
	MaxBufSize int
}

func (t *Thread) AppendBuffer(logger *log.Logger, msg ...message.Message) {
	if t.MsgBuffer.Len()+len(msg) >= t.MaxBufSize {
		logger.Traceln("dumping buffer because it exceeded max size")
		t.DumpBuffer(logger)
	} else {
		for _, m := range msg {
			t.MsgBuffer.PushBack(m)
		}
	}
}

func (t *Thread) DumpBuffer(logger *log.Logger) {
	err := t.Dumper.Dump(&t.MsgBuffer)
	if errors.Is(err, dumper.DUMPTOOBIG) {
		c := customErrors.NewCriticalError(err)
		c.Wrap(map[string]interface{}{"File": t.Dumper.GetPath(), "MaxSize": t.Dumper.GetMaxSize()})
		logger.Fatalln(c.Error())
	} else if err != nil {
		logger.Errorln(err)
	}
	t.MsgBuffer = deque.Deque[message.Message]{}
}

func (t *Thread) FinishThread(logger *log.Logger) {
	t.IsDone = true
	t.DumpBuffer(logger)
}

func (t *Thread) ExtractBatchFromBuffer(batchSz int) []message.Message {
	var res []message.Message
	for i := 0; i < t.MsgBuffer.Len(); i++ {
		if i >= batchSz {
			break
		}
		res = append(res, t.MsgBuffer.At(i))
	}
	// if there were enough elements we should clean them
	if len(res) == batchSz {
		for i := 0; i < batchSz; i++ {
			t.MsgBuffer.PopFront()
		}
	}
	return res
}
