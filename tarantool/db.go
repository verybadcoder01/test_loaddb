package tarantool

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/datetime"
)

type Tuple struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint // that's the doc
	Value    string
	Time     datetime.Datetime
}

type Tarantool struct {
	space          *tarantool.Space
	massInsertFunc string
	conn           *tarantool.Connection
	opts           *tarantool.Opts
	Logger         *log.Logger
}

func NewConnection(logger *log.Logger, name, host, massInsertionFunc string, opts tarantool.Opts) Tarantool {
	conn, err := tarantool.Connect(host, opts)
	if err != nil {
		log.Fatal(err)
	}
	space := tarantool.Space{Name: name, Id: 1}
	return Tarantool{space: &space, conn: conn, Logger: logger, opts: &opts, massInsertFunc: massInsertionFunc}
}

func (t *Tarantool) Close() error {
	return t.conn.Close()
}

// SetupSpace I will leave it here in case I ever need it, but it is awful and this should be done in another way
func (t *Tarantool) SetupSpace() {
	_, err := t.conn.Do(tarantool.NewCallRequest("box.schema.space.create").Args([]interface{}{
		t.space.Name, map[string]bool{"if_not_exists": true},
	})).Get()
	if err != nil {
		t.Logger.Fatalln(err)
	}
	_, err = t.conn.Do(tarantool.NewCallRequest(fmt.Sprintf("box.space.%s:format", t.space.Name)).Args([][]map[string]string{
		{
			{
				"name": "messages", "type": "string",
			},
			{
				"name": "time", "type": "datetime",
			},
		},
	})).Get()
	if err != nil {
		t.Logger.Fatalln(err)
	}
	_, err = t.conn.Do(tarantool.NewCallRequest(fmt.Sprintf("box.space.%s:create_index", t.space.Name)).Args([]interface{}{
		"primary", map[string]interface{}{
			"parts":         []string{"messages"},
			"if_not_exists": true,
		},
	}),
	).Get()
	if err != nil {
		t.Logger.Fatalln(err)
	}
	conn, err := tarantool.Connect(t.conn.Addr(), *t.opts)
	if err != nil {
		t.Logger.Fatalln(err)
	}
	t.conn = conn
	t.Logger.Infoln("tarantool setup finished")
}

func (t *Tarantool) InsertData(data []Tuple) error {
	_, err := t.conn.Do(tarantool.NewCallRequest(fmt.Sprintf("box.func.%s:call", t.massInsertFunc)).Args([]interface{}{
		[]interface{}{t.space.Name, data},
	})).Get()
	return err
}
