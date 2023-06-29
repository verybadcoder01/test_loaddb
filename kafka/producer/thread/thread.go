package thread

import (
	"dbload/kafka/message"
	log "github.com/sirupsen/logrus"
	"os"
)

type Status int

const (
	OK Status = iota
	DEAD
	FINISHED
)

type Thread struct {
	IsDone      bool
	StatusChan  chan Status
	MsgBuffer   []message.Message
	DumpPath    string
	MaxBufSize  int
	MaxDumpSize int
}

func (t *Thread) AppendBuffer(msg ...message.Message) {
	if len(t.MsgBuffer)+len(msg) >= t.MaxBufSize {
		log.Traceln("dumping buffer because it exceeded max size")
		t.DumpBuffer()
	} else {
		t.MsgBuffer = append(t.MsgBuffer, msg...)
	}
}

func (t *Thread) DumpBuffer() {
	file, err := os.OpenFile(t.DumpPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	info, _ := file.Stat()
	// info.Size() is in bytes, so divide by 1024 twice to convert to megabytes
	if info.Size()/1024/1024 > int64(t.MaxDumpSize) {
		log.Fatalf("dump file %v has exceeded it's max size of %v megabytes! Stopping the program!", t.DumpPath, t.MaxDumpSize)
	}
	defer file.Close()
	if err != nil {
		log.Errorln(err)
	}
	for _, msg := range t.MsgBuffer {
		_, err = file.Write([]byte(msg.GetValueForDump()))
		if err != nil {
			log.Errorf("can't dump data %v because %v", msg.GetValueForDump(), err.Error())
		}
	}
	t.MsgBuffer = []message.Message{}
}

func (t *Thread) FinishThread() {
	t.IsDone = true
	t.DumpBuffer()
}

func (t *Thread) GetBatchFromBuffer(batchSz int) []message.Message {
	var res []message.Message
	for i, msg := range t.MsgBuffer {
		if i >= batchSz {
			break
		}
		res = append(res, msg)
	}
	return res
}
