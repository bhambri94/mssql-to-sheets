package main

import (
	"fmt"
	"time"

	"github.com/bhambri94/mssql-to-sheets/configs"
	"github.com/bhambri94/mssql-to-sheets/db"
	"github.com/bhambri94/mssql-to-sheets/sheets"
)

func main() {
	configs.SetConfig()
	var fromDateTime string
	if configs.Configurations.OldDataRequired {
		currentTime := time.Date(2019, time.January, 1, 18, 59, 59, 0, time.UTC) //This can be used to manually fill a sheet from desired date
		fromDateTime = currentTime.Format("2006-01-02 15:04:05")
	} else {
		// loc, _ := time.LoadLocation("Asia/Kolkata") .In(loc)
		currentTime := time.Now()
		HoursCount := 12
		fromDateTime = currentTime.Add(time.Duration(-HoursCount) * time.Hour).Format("2006-01-02 15:04:05")
	}
	fmt.Println("Fetching results from Date: " + fromDateTime)

	finalValues := db.GetLatestDataFromSQL(fromDateTime)
	sheets.BatchAppend(configs.Configurations.SheetNameWithRange, finalValues)
}
