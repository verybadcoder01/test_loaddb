package dumper

import (
	"dbload/kafka/message"
	"errors"
	"github.com/gammazero/deque"
	"os"
)

var DUMPTOOBIG = errors.New("dump file has exceeded it's max size! Stopping the program")

type Dumper interface {
	Dump(buffer *deque.Deque[message.Message]) error
	GetPath() string
	GetMaxSize() int64
}

type SimpleDumper struct {
	File    string
	MaxSize int64
}

func (d *SimpleDumper) GetMaxSize() int64 {
	return d.MaxSize
}

func (d *SimpleDumper) GetPath() string {
	return d.File
}

func (d *SimpleDumper) Dump(buffer *deque.Deque[message.Message]) error {
	file, err := os.OpenFile(d.File, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	info, _ := file.Stat()
	// info.Stat() is in bytes, so to convert to MB we divide twice
	if info.Size()/1024/1024 > d.MaxSize {
		return DUMPTOOBIG
	}
	defer file.Close()
	for i := 0; i < buffer.Len(); i++ {
		msg := (*buffer).At(i)
		_, err = file.Write([]byte(msg.GetValueForDump()))
		if err != nil {
			return err
		}
	}
	return nil
}

func NewDumper(path string, maxSize int64) Dumper {
	return &SimpleDumper{File: path, MaxSize: maxSize}
}
