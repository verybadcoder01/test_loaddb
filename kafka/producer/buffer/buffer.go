package buffer

import (
	"errors"

	"dbload/kafka/message"
	"dbload/kafka/producer/customerrors"
	"github.com/gammazero/deque"
	log "github.com/sirupsen/logrus"
)

type MessageBuffer interface {
	Append(logger *log.Logger, msg ...message.Message)
	ExtractBatch(batchSz int) []message.Message
	Dump(logger *log.Logger)
	Len() int
	GetMaxLen() int
	At(i int) message.Message
}

type DequeBuffer struct {
	buf    deque.Deque[message.Message]
	dumper Dumper
	maxLen int
}

func NewDequeBuffer(d Dumper, mLen int) MessageBuffer {
	return &DequeBuffer{buf: deque.Deque[message.Message]{}, dumper: d, maxLen: mLen}
}

func (d *DequeBuffer) GetMaxLen() int {
	return d.maxLen
}

func (d *DequeBuffer) Len() int {
	return d.buf.Len()
}

func (d *DequeBuffer) Append(logger *log.Logger, msg ...message.Message) {
	if d.Len()+len(msg) >= d.maxLen {
		logger.Traceln("dumping buffer because it exceeded max size")
		d.Dump(logger)
	} else {
		for _, m := range msg {
			d.buf.PushBack(m)
		}
	}
}

func (d *DequeBuffer) At(i int) message.Message {
	return d.buf.At(i)
}

func (d *DequeBuffer) Dump(logger *log.Logger) {
	err := d.dumper.Dump(d)
	if errors.Is(err, ErrDumpTooBig) {
		c := customerrors.NewCriticalError(err)
		c.Wrap(map[string]interface{}{"File": d.dumper.GetPath(), "MaxSize": d.dumper.GetMaxSize()})
		logger.Fatalln(c.Error())
	} else if err != nil {
		logger.Errorln(err)
	}
	d.buf = deque.Deque[message.Message]{}
}

func (d *DequeBuffer) ExtractBatch(batchSz int) []message.Message {
	var res []message.Message
	for i := 0; i < d.Len(); i++ {
		if i >= batchSz {
			break
		}
		res = append(res, d.buf.At(i))
	}
	// if there was enough elements we should clean them
	if len(res) == batchSz {
		for i := 0; i < batchSz; i++ {
			d.buf.PopFront()
		}
	}
	return res
}
