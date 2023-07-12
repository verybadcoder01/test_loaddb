package filler

import (
	"context"
	"math/rand"
	"time"

	"dbload/postgres/models"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func Fill(ctx context.Context, db *gorm.DB, logger *log.Logger) {
	err := db.Create(&models.SupportTable{ID: 1, State: models.NOTPROCCESSED})
	if err != nil {
		logger.Errorln(err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err = db.Create(&models.MainTable{Value: RandString(100)})
			if err != nil {
				logger.Errorln(err)
			}
		}
	}
}
