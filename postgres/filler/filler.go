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
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func Fill(ctx context.Context, db database.Database) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// TODO: switch to creation in batches for performance
			db.InsertMessage(&message.SimpleMessage{Value: RandString(100)})
		}
	}
}
