package migrator

import (
	"dbload/postgres/models"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

func InitTables(db *gorm.DB, logger *log.Logger) {
	if err := db.AutoMigrate(&models.MainTable{}, &models.SupportTable{}); err != nil {
		logger.Errorln(err)
	}
}
