package thread

import (
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
	IsDone     bool
	StatusChan chan Status
	MsgBuffer  []string
	DumpPath   string
	MaxBufSize int
}

func (t *Thread) AppendBuffer(msg ...string) {
	if len(t.MsgBuffer)+len(msg) >= t.MaxBufSize {
		log.Traceln("dumping buffer because it exceeded max size")
		t.DumpBuffer()
	} else {
		t.MsgBuffer = append(t.MsgBuffer, msg...)
	}
}

func (t *Thread) DumpBuffer() {
	file, err := os.OpenFile(t.DumpPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	defer file.Close()
	if err != nil {
		log.Errorln(err)
	}
	for _, msg := range t.MsgBuffer {
		_, err = file.Write([]byte(msg))
		if err != nil {
			log.Errorf("can't dump data %v because %v", msg, err.Error())
		}
	}
	t.MsgBuffer = []string{}
}
func (t *Thread) FinishThread() {
	t.IsDone = true
	t.DumpBuffer()
}
