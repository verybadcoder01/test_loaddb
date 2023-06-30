package buffer

import (
	"errors"

	"dbload/kafka/message"
	"dbload/kafka/producer/customErrors"
	"dbload/kafka/producer/dumper"
	"github.com/gammazero/deque"
	log "github.com/sirupsen/logrus"
)

type MessageBuffer interface {
	Append(logger *log.Logger, msg ...message.Message)
	ExtractBatch(batchSz int) []message.Message
	Dump(logger *log.Logger)
	Len() int
	GetMaxLen() int
}

type DequeBuffer struct {
	Buf    deque.Deque[message.Message]
	Dumper dumper.Dumper
	MaxLen int
}

func (d *DequeBuffer) GetMaxLen() int {
	return d.MaxLen
}

func (d *DequeBuffer) Len() int {
	return d.Buf.Len()
}

func (d *DequeBuffer) Append(logger *log.Logger, msg ...message.Message) {
	if d.Len()+len(msg) >= d.MaxLen {
		logger.Traceln("dumping buffer because it exceeded max size")
		d.Dump(logger)
	} else {
		for _, m := range msg {
			d.Buf.PushBack(m)
		}
	}
}

func (d *DequeBuffer) Dump(logger *log.Logger) {
	err := d.Dumper.Dump(&d.Buf)
	if errors.Is(err, dumper.DUMPTOOBIG) {
		c := customErrors.NewCriticalError(err)
		c.Wrap(map[string]interface{}{"File": d.Dumper.GetPath(), "MaxSize": d.Dumper.GetMaxSize()})
		logger.Fatalln(c.Error())
	} else if err != nil {
		logger.Errorln(err)
	}
	d.Buf = deque.Deque[message.Message]{}
}

func (d *DequeBuffer) ExtractBatch(batchSz int) []message.Message {
	var res []message.Message
	for i := 0; i < d.Len(); i++ {
		if i >= batchSz {
			break
		}
		res = append(res, d.Buf.At(i))
	}
	// if there was enough elements we should clean them
	if len(res) == batchSz {
		for i := 0; i < batchSz; i++ {
			d.Buf.PopFront()
		}
	}
	return res
}
