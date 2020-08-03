package db

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/bhambri94/mssql-to-sheets/configs"
	_ "github.com/denisenkom/go-mssqldb"
)

func GetLatestDataFromSQL(fromDateTime string) [][]interface{} {
	connectString := "sqlserver://" + configs.Configurations.UserName + ":" + configs.Configurations.Password + "@" + configs.Configurations.MSSQLHost + "?database=" + configs.Configurations.DatabaseName + "&connection+timeout=30"

	println("opening sql connection with connstring:" + connectString)
	DBConnection, err := sql.Open("mssql", connectString)
	defer DBConnection.Close()
	if err != nil {
		println("Open Error:", err)
		log.Fatal(err)
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
		singleRow := make([]interface{}, 12)
		if err := Rows.Scan(&singleRow[0], &singleRow[1], &singleRow[2], &singleRow[3], &singleRow[4], &singleRow[5], &singleRow[6], &NumericToString, &singleRow[8], &singleRow[9], &singleRow[10], &singleRow[11]); err != nil {
			log.Fatal(err)
		}
		singleRow[7] = B2S(NumericToString)
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
