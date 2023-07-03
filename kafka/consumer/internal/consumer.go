package internal

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"dbload/kafka/config"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

// HandleSignals not sure if it will work on windows
func HandleSignals(reader *kafka.Reader) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	var err error
	go func() {
		<-c
		err = reader.Close()
	}()
	return err
}

func consume(ctx context.Context, wg *sync.WaitGroup, conf *config.Config, logger *log.Logger) {
	defer wg.Done()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{conf.Kafka}, Topic: conf.KafkaTopic, StartOffset: kafka.FirstOffset,
		Partition: conf.KafkaPartition, MaxBytes: 10e6, MinBytes: 10e3,
	})
	err := HandleSignals(reader)
	if err != nil {
		logger.Errorln(err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if msg, err := reader.ReadMessage(context.Background()); err != nil {
				logger.Errorln("Error reading Kafka:", err)
				break
			} else {
				logger.Infof("topic=%s, partition=%d, offset=%d, key=%s, value=%s", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
		}
	}
}

func StartConsuming(conf *config.Config, logger *log.Logger) {
	wg := sync.WaitGroup{}
	wg.Add(conf.MaxThreads)
	// context leaves place for further management
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < conf.MaxThreads; i++ {
		go consume(ctx, &wg, conf, logger)
	}
	wg.Wait()
	cancel()
}
