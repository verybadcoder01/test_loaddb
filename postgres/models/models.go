package models

import "gorm.io/gorm"

type Status int

const (
	NOTPROCCESSED Status = iota + 1
	READ
	INPROCESS
	OK
)

type MainTable struct {
	gorm.Model
	Value string
}

type SupportTable struct {
	ID    int `gorm:"primaryKey"`
	State Status
}
