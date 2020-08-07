package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/bhambri94/mssql-to-sheets/configs"
	_ "github.com/denisenkom/go-mssqldb"
)

func GetLatestDataFromSQL(fromDateTime string) [][]interface{} {
	var DBConnection *sql.DB
	var err error
	connectString := "sqlserver://" + configs.Configurations.UserName + ":" + configs.Configurations.Password + "@" + configs.Configurations.MSSQLHost + "?database=" + configs.Configurations.DatabaseName + "&connection+timeout=300"
	println("opening sql connection with connstring:" + connectString)

	RetryCounter := 0
	ConnectionSuccess := false
	for RetryCounter < 5 && !ConnectionSuccess {
		DBConnection, err = sql.Open("mssql", connectString)
		defer DBConnection.Close()
		if err != nil {
			RetryCounter++
			if strings.Contains(err.Error(), "Client.Timeout") {
			} else {
				println("Open Error:", err)
				log.Fatal(err)
			}
		} else {
			ConnectionSuccess = true
		}
	}

	println("Running Query -> " + configs.Configurations.Query + " where " + configs.Configurations.DateColumnName + " > " + fromDateTime + " & scan")
	Rows, err := DBConnection.Query(configs.Configurations.Query + " where " + configs.Configurations.DateColumnName + " > '" + fromDateTime + "'" + " order by " + configs.Configurations.DateColumnName + " asc")
	if err != nil {
		log.Fatal(err)
	}

	var finalValues [][]interface{}
	for Rows.Next() {
		fmt.Println("adding rows to finalValues")
		var NumericToString []uint8
		var QuotationDate string
		var LostDate string
		singleRow := make([]interface{}, 12)
		if err := Rows.Scan(&singleRow[0], &singleRow[1], &singleRow[2], &QuotationDate, &singleRow[4], &singleRow[5], &singleRow[6], &NumericToString, &singleRow[8], &singleRow[9], &singleRow[10], &LostDate); err != nil {
			log.Fatal(err)
		}
		singleRow[7] = B2S(NumericToString)
		singleRow[3] = QuotationDate[:10]
		singleRow[11] = LostDate[:10]
		finalValues = append(finalValues, singleRow)
	}

	println("closing connection")
	DBConnection.Close()
	return finalValues

}

func B2S(bs []uint8) string {
	ba := []byte{}
	for _, b := range bs {
		ba = append(ba, byte(b))
	}
	return string(ba)
}
