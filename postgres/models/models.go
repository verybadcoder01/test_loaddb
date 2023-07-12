package models

import (
	"gorm.io/gorm"
)

type MainTable struct {
	gorm.Model
	Value string
}

type SupportTable struct {
	ID    int `gorm:"primaryKey;autoIncrement:false"`
	ValID int
}
