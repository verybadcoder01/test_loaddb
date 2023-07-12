package database

import (
	"errors"

	"dbload/kafka/message"
	"dbload/postgres/models"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Database interface {
	FillSupportTable(cnt int, step int)
	InitTables()
	GetMessages(threadID int, size int) []message.Message
	InsertMessage(m message.Message)
}

type PgDatabase struct {
	db     *gorm.DB
	Logger *log.Logger
}

func NewPgDatabase(dsn string, logger *log.Logger) Database {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		logger.Fatalln(err)
	}
	return &PgDatabase{db: db, Logger: logger}
}

func (db *PgDatabase) InitTables() {
	if err := db.db.AutoMigrate(&models.MainTable{}, &models.SupportTable{}); err != nil {
		db.Logger.Errorln(err)
	}
}

func (db *PgDatabase) FillSupportTable(cnt int, step int) {
	for i := 0; i < cnt; i++ {
		// 0, step, 2 * step, ...
		db.db.Create(&models.SupportTable{ID: i + 1, ValID: 1 + i*step})
	}
}

func (db *PgDatabase) InsertMessage(m message.Message) {
	err := db.db.Create(&models.MainTable{Value: m.GetValueForDump()})
	if err.Error != nil {
		db.Logger.Errorln(err.Error.Error())
	}
}

func countGood(arr []models.MainTable) int {
	cnt := 0
	for _, i := range arr {
		if i.Value != "" {
			cnt++
		}
	}
	return cnt
}

func (db *PgDatabase) GetMessages(threadID int, size int) []message.Message {
	final := make([]message.Message, size)
	var res []models.MainTable
	var cur models.SupportTable
	err := db.db.First(&cur, threadID+1)
	if errors.Is(err.Error, gorm.ErrRecordNotFound) {
		db.Logger.Fatalln("support table corrupted: " + err.Error.Error())
	} else if err.Error != nil {
		db.Logger.Errorln(err.Error.Error())
	}
	err = db.db.Find(&res, "id>=? AND id<?", cur.ValID, cur.ValID+size)
	for countGood(res) < size {
		err = db.db.Find(&res, "id>=? AND id<?", cur.ValID, cur.ValID+size)
		if err.Error != nil {
			db.Logger.Errorln(err.Error.Error())
		}
	}
	for i, msg := range res {
		final[i] = &message.SimpleMessage{Value: msg.Value}
	}
	cur.ValID = cur.ValID + size
	db.db.Save(&cur)
	return final
}
