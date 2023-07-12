package reader

import (
	"dbload/kafka/message"
	"dbload/postgres/models"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

func GetMessage(db *gorm.DB, logger *log.Logger) message.Message {
	var res models.SupportTable
	err := db.First(&res)
	if err.Error != nil {
		logger.Fatalf("support table inaccessible: %v", err.Error.Error())
	}
	var value models.MainTable
	err = db.First(&value, res.ID)
	if err.Error != nil {
		logger.Errorln(err.Error.Error())
	}
	err = db.Create(&models.SupportTable{State: models.NOTPROCCESSED})
	err = db.Delete(&res)
	if err.Error != nil {
		logger.Errorln(err.Error.Error())
	}
	return &message.SimpleMessage{Value: value.Value}
}
