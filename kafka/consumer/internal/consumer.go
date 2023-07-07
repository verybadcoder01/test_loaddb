package internal

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

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
				logger.Infof("topic=%s, partition=%d, offset=%d, key=%s, value=%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
		}
	}
}

func StartConsuming(maxThreads int, logger *log.Logger, reader *kafka.Reader) {
	defer func() {
		if err := reader.Close(); err != nil {
			logger.Errorln(err)
		}
	}()
	wg := sync.WaitGroup{}
	wg.Add(maxThreads)
	// context leaves place for further management
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < maxThreads; i++ {
		go func() { consume(ctx, logger, reader); wg.Done() }()
	}
	wg.Wait()
	cancel()
}
