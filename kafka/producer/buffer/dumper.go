package buffer

import (
	"errors"
	"os"
)

var ErrDumpTooBig = errors.New("dump file has exceeded it's max size! Stopping the program")

type Dumper interface {
	Dump(buffer MessageBuffer) error
	GetPath() string
	GetMaxSize() int64
}

type SimpleDumper struct {
	file    string
	maxSize int64
}

func (d *SimpleDumper) GetMaxSize() int64 {
	return d.maxSize
}

func (d *SimpleDumper) GetPath() string {
	return d.file
}

func (d *SimpleDumper) Dump(buffer MessageBuffer) error {
	file, err := os.OpenFile(d.file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	info, _ := file.Stat()
	// info.Stat() is in bytes, so to convert to MB we divide twice
	if info.Size()/1024/1024 > d.maxSize {
		return ErrDumpTooBig
	}
	defer file.Close()
	for i := 0; i < buffer.Len(); i++ {
		msg := buffer.At(i)
		_, err = file.Write([]byte(msg.GetValueForDump()))
		if err != nil {
			return err
		}
	}
	return nil
}

func NewSimpleDumper(path string, maxSize int64) Dumper {
	return &SimpleDumper{file: path, maxSize: maxSize}
}
