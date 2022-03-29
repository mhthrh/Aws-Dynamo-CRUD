package main

import (
	"fmt"
	"github.com/mhthrh/Aws-Dynamo/AwsDynamo"
	"github.com/mhthrh/Aws-Dynamo/Entity"
)

func main() {
	entity := Entity.Entity{
		ID:        1,
		Title:     "AwsDynamo",
		FirstName: "Mohsen",
		SureName:  "Taheri",
		Email:     "mhthrh@gmail.com",
		CellNo:    "0987654321",
	}
	db := AwsDynamo.New()
	//--------------------------------
	db.Insert(entity)
	//--------------------------------
	db.CreateTable()
	//--------------------------------
	db.Select()
	//--------------------------------
	db.Update("", "", "")
	//--------------------------------
	db.Delete("", "")
	//--------------------------------
	tables, err := db.LoadTables()
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, i := range tables {
		fmt.Println(i)
	}

}
