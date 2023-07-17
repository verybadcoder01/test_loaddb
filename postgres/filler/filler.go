package filler

import (
	"context"
	"math/rand"
	"time"

	"dbload/kafka/message"
	"dbload/postgres/database"
)

func init() {
	rand.Seed(time.Now().UnixNano()) //nolint // we don't care
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))] //nolint //the random doesn't matter here!
	}
	return string(b)
}

func Fill(ctx context.Context, db database.Database, createBatchSize int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var batch []message.Message
			for i := 0; i < createBatchSize; i++ {
				batch = append(batch, &message.TimestampedMessage{Value: RandString(100)})
			}
			db.InsertMessages(batch)
		}
	}
}
