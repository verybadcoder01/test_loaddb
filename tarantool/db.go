package tarantool

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tarantool/go-tarantool/v2"
)

type Tuple struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint // that's the doc
	Value    string
	Time     time.Time
}

type Tarantool struct {
	space  *tarantool.Space
	conn   *tarantool.Connection
	opts   *tarantool.Opts
	Logger *log.Logger
}

func NewSpace(logger *log.Logger, name string, host string, opts tarantool.Opts) Tarantool {
	conn, err := tarantool.Connect(host, opts)
	if err != nil {
		log.Fatal(err)
	}
	space := tarantool.Space{Name: name, Id: 1}
	return Tarantool{space: &space, conn: conn, Logger: logger, opts: &opts}
}

func (t *Tarantool) Close() error {
	return t.conn.Close()
}

func (t *Tarantool) SetupSpace() {
	fut := t.conn.Do(tarantool.NewCallRequest("box.schema.space.create").Args([]interface{}{
		t.space.Name, map[string]bool{"if_not_exists": true},
	}))
	<-fut.WaitChan()
	if fut.Err() != nil {
		t.Logger.Fatalln(fut.Err().Error())
	}
	fut = t.conn.Do(tarantool.NewCallRequest(fmt.Sprintf("box.space.%s:format", t.space.Name)).Args([][]map[string]string{
		{
			{
				"name": "messages", "type": "string",
			},
			{
				"name": "time", "type": "datetime",
			},
		},
	}))
	<-fut.WaitChan()
	if fut.Err() != nil {
		t.Logger.Fatalln(fut.Err().Error())
	}
	fut = t.conn.Do(tarantool.NewCallRequest(fmt.Sprintf("box.space.%s:create_index", t.space.Name)).Args([]interface{}{
		"primary", map[string]bool{"if_not_exists": true},
	}))
	<-fut.WaitChan()
	if fut.Err() != nil {
		t.Logger.Fatalln(fut.Err().Error())
	}
	conn, err := tarantool.Connect(t.conn.Addr(), *t.opts)
	if err != nil {
		t.Logger.Fatalln(err)
	}
	t.conn = conn
	t.Logger.Infoln("tarantool setup correct")
}

func (t *Tarantool) InsertData(data []Tuple) error {
	for _, d := range data {
		_, err := t.conn.Do(tarantool.NewInsertRequest(t.space.Name).Tuple(&d)).Get()
		if err != nil {
			return err
		}
	}
	return nil
}
