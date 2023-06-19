package models

import (
	"gorm.io/datatypes"
)

type FOrders struct {
	Id                uint `gorm:"primaryKey"`
	CallTime          datatypes.Time
	PhoneNumber       string
	ClientId          uint `gorm:"foreignKey:DClients.Id"`
	Address           string
	Info              string
	DeliveryTime      datatypes.Time
	Price             float32
	MealCountDiscount float32
	ClientDiscount    float32
	FinalPrice        float32
}

type LOrdersMeals struct {
	Id        uint `gorm:"primaryKey"`
	FOrdersId uint `gorm:"foreignKey:FOrdersId"`
	DMealsId  uint `gorm:"foreignKey:DMealsId"`
	Count     uint
}

type DMeals struct {
	Id          uint `gorm:"primaryKey"`
	Name        string
	Description string
	Ingredients string
	Price       float32
}

type DDiscounts struct {
	Id     uint `gorm:"primaryKey"`
	Value  float32
	Reason string
}

type DClients struct {
	Id           uint `gorm:"primaryKey"`
	Name         string
	PhoneNumber  string
	OrderCount   uint
	DiscountType uint `gorm:"foreignKey:DDiscountsId"`
}

type DenormalizedExcel struct {
	OrderId           uint
	CallTime          datatypes.Time
	OrderPhone        string
	Address           string
	DeliveryTime      datatypes.Time
	OrderPrice        float32
	MealCountDiscount float32
	FinalPrice        float32
	MealId            uint
	MealCount         uint
	MealDesc          string
	Ingredients       string
	MealPrice         float32
	ClientId          uint
	ClientName        string
	ClientPhone       string
	ClientOrderCnt    uint
	DiscountId        uint
	DiscountValue     float32
	DiscountReason    string
}
