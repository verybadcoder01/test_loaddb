package internal

import (
	"context"
	"fmt"
	"sync"

	tarantooldb "dbload/tarantool"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/tarantool/go-tarantool/v2/datetime"
)

func consume(ctx context.Context, db *tarantooldb.Tarantool, reader *kafka.Reader, batchSz int, maxMsg int) {
	var batch []tarantooldb.Tuple
outer:
	for i := 0; i < maxMsg; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			if msg, err := reader.ReadMessage(ctx); err != nil {
				db.Logger.Errorln("Error reading Kafka:", err)
				break outer
			} else {
				dt, err := datetime.MakeDatetime(msg.Time)
				if err != nil {
					log.Errorln("can not parse time to datetime: " + err.Error())
				}
				batch = append(batch, tarantooldb.Tuple{
					Time:  dt,
					Value: fmt.Sprintf("value=%s", msg.Value),
				})
				if len(batch) >= batchSz {
					err = db.InsertData(batch)
					if err != nil {
						db.Logger.Errorf("tarantool error: %s", err.Error())
					} else {
						batch = []tarantooldb.Tuple{}
						db.Logger.Infoln("write batch to tarantool")
					}
				}
			}
		}
	}
}

func StartConsuming(ctx context.Context, db *tarantooldb.Tarantool, maxThreads, tBatchSz, maxMsg int, reader *kafka.Reader) {
	wg := sync.WaitGroup{}
	wg.Add(maxThreads)
	// context leaves place for further management
	ctx, cancel := context.WithCancel(ctx)
	for i := 0; i < maxThreads; i++ {
		go func() { consume(ctx, db, reader, tBatchSz, maxMsg); wg.Done() }()
	}
	wg.Wait()
	cancel()
}
