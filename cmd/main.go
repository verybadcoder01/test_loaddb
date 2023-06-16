package main

import (
	"dbload/models"
	"fmt"
	"github.com/xuri/excelize/v2"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
	"reflect"
	"strconv"
	"strings"
)

const (
	sheet           = "Лист1"
	alphabet        = "ABCDEFGHIJKLMNOPQRST"
	FIRSTROW        = 2
	ROWSCOUNT       = 7
	fileName        = "sample.xlsx"
	DISCOUNTTYPE50  = 15 //50 заказов - 15% скидка
	DISCOUNTTYPE100 = 20 //100 заказов - 20% скидка
)

func NewOrder(row *models.DenormalizedExcel) models.FOrders {
	var order models.FOrders
	order.Id = row.OrderId
	order.CallTime = row.CallTime
	order.ClientId = row.ClientId
	order.Address = row.Address
	order.DeliveryTime = row.DeliveryTime
	order.PhoneNumber = row.OrderPhone
	order.Price = row.OrderPrice
	order.FinalPrice = row.FinalPrice
	order.MealCountDiscount = row.MealCountDiscount
	order.ClientDiscount = row.DiscountValue
	return order
}

func NewMeal(row *models.DenormalizedExcel) models.DMeals {
	var meal models.DMeals
	meal.Id = row.MealId
	meal.Name = row.MealDesc
	meal.Description = row.MealDesc
	meal.Price = row.MealPrice
	meal.Ingredients = row.Ingredients
	return meal
}

func NewDiscountType(row *models.DenormalizedExcel) models.DDiscounts {
	var discount models.DDiscounts
	discount.Id = row.DiscountId
	discount.Value = row.DiscountValue
	discount.Reason = row.DiscountReason
	return discount
}

func NewClient(row *models.DenormalizedExcel) models.DClients {
	var client models.DClients
	client.Id = row.ClientId
	client.Name = row.ClientName
	client.PhoneNumber = row.ClientPhone
	client.OrderCount = row.ClientOrderCnt
	if client.OrderCount < 100 && client.OrderCount >= 50 {
		client.DiscountType = 1
	} else if client.OrderCount > 100 {
		client.DiscountType = 2
	} else {
		client.DiscountType = 0
	}
	return client
}

func main() {
	db, err := gorm.Open(postgres.Open("postgres://user:password@localhost:5432/pizza_shop"), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: false, IgnoreRelationshipsWhenMigrating: false})
	if err != nil {
		panic(err)
	}
	err = db.AutoMigrate(&models.FOrders{}, &models.LOrdersMeals{}, &models.DMeals{}, &models.DDiscounts{}, &models.DClients{})
	if err != nil {
		panic(err)
	}
	db.Create(&models.DDiscounts{Id: 1, Value: DISCOUNTTYPE50, Reason: "50 заказов"})
	db.Create(&models.DDiscounts{Id: 2, Value: DISCOUNTTYPE100, Reason: "100 заказов"})
	file, err := excelize.OpenFile(fileName)
	defer func(file *excelize.File) {
		err = file.Close()
		if err != nil {
			log.Println(err.Error())
		}
	}(file)
	ordersMeals := make(map[uint][]models.DMeals)
	if err != nil {
		panic(err)
	}
	for row := FIRSTROW; row < ROWSCOUNT; row++ {
		var excelRow models.DenormalizedExcel
		for i, letter := range alphabet {
			val, err := file.GetCellValue(sheet, fmt.Sprintf("%c%d", letter, row))
			if err != nil {
				log.Println(err.Error())
			}
			field := reflect.ValueOf(&excelRow).Elem().Field(i)
			switch field.Kind() {
			case reflect.Uint:
				ival, _ := strconv.Atoi(val)
				field.SetUint(uint64(ival))
			case reflect.String:
				field.SetString(val)
			case reflect.Float32, reflect.Float64:
				dval := strings.Replace(val, ",", ".", 1)
				fval, _ := strconv.ParseFloat(dval, 32)
				field.SetFloat(fval)
			default:
				sepInd := strings.Index(val, ":")
				h, _ := strconv.Atoi(val[:sepInd])
				m, _ := strconv.Atoi(val[sepInd+1:])
				field.Set(reflect.ValueOf(datatypes.NewTime(h, m, 0, 0)))
			}
		}
		order := NewOrder(&excelRow)
		db.Create(&order)
		meal := NewMeal(&excelRow)
		db.Create(&meal)
		ordersMeals[order.Id] = append(ordersMeals[order.Id], meal)
		discount := NewDiscountType(&excelRow)
		if discount.Value != 0 {
			db.Create(&discount)
		}
		client := NewClient(&excelRow)
		db.Create(&client)
		for _, meal := range ordersMeals[order.Id] {
			db.Create(&models.LOrdersMeals{FOrdersId: order.Id, DMealsId: meal.Id, Count: excelRow.MealCount})
		}
		log.Println(excelRow)
	}
}
