package database

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"

	_ "github.com/go-sql-driver/mysql"
)

func ExtractTableNamesFromSQLFile(filepath string) ([]string, error) {
	content, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read the SQL file, %v", err)
	}

	//regex to match "CREATE TABLE "table_name"
	re := regexp.MustCompile(`(?i)CREATE TABLE (\w+)`)
	matches := re.FindAllStringSubmatch(string(content), -1)

	var tableNames []string
	for _, match := range matches {
		if len(match) > 1 {
			tableNames = append(tableNames, match[1])
		}
	}
	return tableNames, nil
}

func ConnectMySQL(user, password, host string, port int, dbname string) (*sql.DB, error) {
	// DSN for MySQL
	//format: user:password@tcp(host:port)/name
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", user, password, host, port, dbname)

	//open connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection %v", err)
	}
	fmt.Println("Successfully connected to MySQL database... ")
	return db, err
}

// retreives data from the mysql table and returns it has a slice of maps
func FetchData(db *sql.DB, sqlFilepath string) ([]map[string]interface{}, error) {
	//Extracting table names from the sql file provided
	tableNames, err := ExtractTableNamesFromSQLFile(sqlFilepath)
	if err != nil {
		return nil, fmt.Errorf("failed to extract table names %v", err)
	}

	if len(tableNames) == 0 {
		return nil, fmt.Errorf("no tables found in the sql file, %v", err)
	}

	var allResults []map[string]interface{}

	//to do, make this query generic so that hardcoding can be avoided,
	for _, tableName := range tableNames {
		query := fmt.Sprintf("SELECT * FROM %s;", tableName) // using 'users' as the hardcoded table name

		//execute query
		rows, err := db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to execute the query on table %s, %v", tableName, err)
		}
		defer rows.Close()

		//get column names
		columns, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("failed to get column names %v", err)
		}

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
			allResults = append(allResults, rowMap)
		}
	}

	//return all rows
	return allResults, nil
}
