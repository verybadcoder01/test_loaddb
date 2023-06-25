package thread

import (
	"log"
	"os"
)

type Status int

const (
	OK Status = iota
	DEAD
	FINISHED
)

type Thread struct {
	IsDone       bool
	CriticalChan chan error
	StatusChan   chan Status
	MsgBuffer    []string
	DumpPath     string
	MaxBufSize   int
}

func (t *Thread) AppendBuffer(msg string) {
	if len(t.MsgBuffer) >= t.MaxBufSize {
		t.DumpBuffer()
		t.MsgBuffer = []string{}
	} else {
		t.MsgBuffer = append(t.MsgBuffer, msg)
	}
}

func (t *Thread) DumpBuffer() {
	file, err := os.OpenFile(t.DumpPath, os.O_WRONLY|os.O_APPEND|os.O_RDONLY, 0666)
	if err != nil {
		log.Println(err)
	}
	for _, msg := range t.MsgBuffer {
		_, err = file.Write([]byte(msg))
		if err != nil {
			log.Printf("can't dump data %v because %v", msg, err.Error())
		}
	}
	t.MsgBuffer = nil
}
func (t *Thread) FinishThread() {
	t.IsDone = true
	t.DumpBuffer()
}
