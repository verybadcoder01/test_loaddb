package internal

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

var total int64

func consume(ctx context.Context, logger *log.Logger, reader *kafka.Reader) {
outer:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if msg, err := reader.ReadMessage(ctx); err != nil {
				logger.Errorln("Error reading Kafka:", err)
				break outer
			} else {
				atomic.AddInt64(&total, 1)
				logger.Infof("topic=%s, partition=%d, offset=%d, key=%s, value=%s, total=%d", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value, total)
			}
		}
	}
}

func StartConsuming(ctx context.Context, maxThreads int, logger *log.Logger, reader *kafka.Reader) {
	wg := sync.WaitGroup{}
	wg.Add(maxThreads)
	// context leaves place for further management
	ctx, cancel := context.WithCancel(ctx)
	for i := 0; i < maxThreads; i++ {
		go func() { consume(ctx, logger, reader); wg.Done() }()
	}
	wg.Wait()
	cancel()
}
