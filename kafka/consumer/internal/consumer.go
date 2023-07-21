package internal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	tarantooldb "dbload/tarantool"
	"github.com/segmentio/kafka-go"
)

var total int64

func consume(ctx context.Context, db *tarantooldb.Tarantool, reader *kafka.Reader, batchSz int) {
	var batch []tarantooldb.Tuple
outer:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if msg, err := reader.ReadMessage(ctx); err != nil {
				db.Logger.Errorln("Error reading Kafka:", err)
				break outer
			} else {
				atomic.AddInt64(&total, 1)
				batch = append(batch, tarantooldb.Tuple{
					Time:  msg.Time,
					Value: fmt.Sprintf("value=%s, total=%d", msg.Value, total),
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

func StartConsuming(ctx context.Context, db *tarantooldb.Tarantool, maxThreads int, tBatchSz int, reader *kafka.Reader) {
	wg := sync.WaitGroup{}
	wg.Add(maxThreads)
	// context leaves place for further management
	ctx, cancel := context.WithCancel(ctx)
	for i := 0; i < maxThreads; i++ {
		go func() { consume(ctx, db, reader, tBatchSz); wg.Done() }()
	}
	wg.Wait()
	cancel()
}
