package database

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func ConnectMySQL() (*sql.DB, error) {
	//getting cred from environment variables
	user := os.Getenv("MYSQL_USER")
	password := os.Getenv("MYSQL_PASS")
	dbname := os.Getenv("MYSQL_NAME")
	port := os.Getenv("MYSQL_PORT")
	host := os.Getenv("MYSQL_HOST")

	// DSN for MySQL
	//format: user:password@tcp(host:port)/name
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, dbname)

	//open connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection %v", err)
	}
	fmt.Println("Successfully connected to MySQL database... ")
	return db, err
}

// retreives data from the mysql table and returns it has a slice of maps
func FetchData(db *sql.DB) ([]map[string]interface{}, error) {
	//to do, make this query generic so that hardcoding can be avoided,
	query := "SELECT * FROM Employees;" // using 'users' as the hardcoded table name

	//execute query
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute the query %v", err)
	}
	defer rows.Close()

	//get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names %v", err)
	}

	// store results in slice
	var results []map[string]interface{}

	//Iterating through the rows
	for rows.Next() {
		//placeholder interfaces to store values
		values := make([]interface{}, len(columns))
		valuesPtr := make([]interface{}, len(columns))

		//storing corresponding pointer values as row.Scan() works only on pointers
		for i := range values {
			valuesPtr[i] = &values[i]
		}

		//scan the rows into the valuePtr
		//valuesPtr... expands the slice into individual args
		if err := rows.Scan(valuesPtr...); err != nil {
			return nil, fmt.Errorf("failed to scan the row %v", err)
		}

		//create a new map to store the scanned data of the current row
		rowMap := make(map[string]interface{})

		//add column values for this row
		for i, colName := range columns {
			val := values[i]

			//check to see if some values are in bytes, if yes convert them to string
			if b, ok := val.([]byte); ok {
				rowMap[colName] = string(b)
			} else {
				rowMap[colName] = val
			}
		}

		//appending the row to results
		results = append(results, rowMap)
	}

	//return all rows
	return results, nil
}
