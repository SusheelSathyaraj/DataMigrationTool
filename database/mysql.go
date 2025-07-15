package database

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/SusheelSathyaraj/DataMigrationTool/config"

	_ "github.com/go-sql-driver/mysql"
)

// Interface for ease with mock tests
type DatabaseClient interface {
	Connect() error
	Close() error
	FetchAllData(tables []string) ([]map[string]interface{}, error)
	ExecuteQuery(query string) (*sql.Rows, error)
	ImportData(data []map[string]interface{}) error
}

type MySQLClient struct {
	User     string
	Password string
	Host     string
	Port     int
	DBName   string
	DB       *sql.DB
}

// create a MySQL client using manual parameters, (for tests)
func NewMySQLClient(user, password, host string, port int, dbname string) *MySQLClient {
	return &MySQLClient{
		User:     user,
		Password: password,
		Host:     host,
		Port:     port,
		DBName:   dbname,
	}
}

// create a new MySQL client using config file
func NewMYSQLClientFromConfig(cfg *config.Config) *MySQLClient {
	return &MySQLClient{
		User:     cfg.MySQL.User,
		Password: cfg.MySQL.Password,
		Host:     cfg.MySQL.Host,
		Port:     cfg.MySQL.Port,
		DBName:   cfg.MySQL.DBName,
	}
}

// to connect with the MySQL DB
func (c *MySQLClient) Connect() error {
	// DSN for MySQL
	//format: user:password@tcp(host:port)/name
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", c.User, c.Password, c.Host, c.Port, c.DBName)

	//open connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open MySQL connection %v", err)
	}

	//test the connection
	if err = db.Ping(); err != nil {
		return fmt.Errorf("failed to ping to the SQL database, %v", err)
	}

	c.DB = db

	fmt.Println("Successfully connected to MySQL database... ")
	return nil
}

// closes the database connection
func (c *MySQLClient) Close() error {
	if c.DB != nil {
		return c.DB.Close()
	}
	return nil
}

// executes the query to return the rows
func (c *MySQLClient) ExecuteQuery(query string) (*sql.Rows, error) {
	if c.DB == nil {
		return nil, fmt.Errorf("db connection not established")
	}
	return c.DB.Query(query)
}

// fetches all data from all the specified tables
func (c *MySQLClient) FetchAllData(tables []string) ([]map[string]interface{}, error) {
	if c.DB == nil {
		return nil, fmt.Errorf("db connection not established")
	}

	var allResults []map[string]interface{}

	for _, tableName := range tables {
		//sanitize table to prevent sql injection
		sanitizedTableName := sanitizeIdentifier(tableName)
		query := fmt.Sprintf("SELECT * FROM %s;", sanitizedTableName)

		results, err := c.fetchDataFromTable(query)
		if err != nil {
			return nil, fmt.Errorf("error fetching data from the table %s: %v", tableName, err)
		}

		//Add table info to each row
		for i := range results {
			results[i]["_source_table"] = tableName
		}
		allResults = append(allResults, results...)
	}
	return allResults, nil
}

// executes a query and returns the result as a slice of maps
func (c *MySQLClient) fetchDataFromTable(query string) ([]map[string]interface{}, error) {
	rows, err := c.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query %v", err)
	}
	defer rows.Close()

	//Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names, %v", err)
	}

	var results []map[string]interface{}

	//iterate through rows
	for rows.Next() {
		//create a slice of interface to hold values
		values := make([]interface{}, len(columns))
		//valuesPtr := make([]interface{}, len(columns))

		//setup pointers
		for i := range values {
			//valuesPtr[i] = values[i]
			values[i] = new(interface{})
		}

		//scan row into the pointers
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		//create a map for this row
		rowMap := make(map[string]interface{})

		//convert any []byte to a string for storing
		for i, colName := range columns {
			//Dereferencing the pointer to get actual value
			val := *(values[i].(*interface{}))
			if b, ok := val.([]byte); ok {
				rowMap[colName] = string(b)
			} else {
				rowMap[colName] = val
			}
		}
		results = append(results, rowMap)
	}
	//Check for error after iterating through rows
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error during the row iteration,%v", err)
	}
	return results, nil
}

func (c *MySQLClient) ImportData(data []map[string]interface{}) error {
	if c.DB == nil {
		return fmt.Errorf("database connection not established")
	}
	if len(data) == 0 {
		return fmt.Errorf("no data to import")
	}

	//Grouping data by table
	tableData := make(map[string][]map[string]interface{}, 0)
	for _, row := range data {
		tableName, okay := row["_source_table"].(string)
		if !okay {
			return fmt.Errorf("row missing source table information")
		}
		tableData[tableName] = append(tableData[tableName], row)
	}

	//processing each table
	for tableName, rows := range tableData {
		if len(rows) == 0 {
			continue
		}
		//get column names apart from _source_table
		first_row := rows[0]
		columns := make([]string, 0, len(first_row)-1)
		for col := range first_row {
			if col != "_source_table" {
				columns = append(columns, col)
			}
		}

		//Designing Transaction
		tx, err := c.DB.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction, %v", err)
		}

		//Creating table if not present
		createTableSQL := generateMySQLCreateTableSQL(tableName, first_row)
		_, err = tx.Exec(createTableSQL)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to create a table %s, %v", tableName, err)
		}

		//Preparing insert statement
		placeholder := make([]string, len(columns))
		for i := range placeholder {
			placeholder[i] = "?"
		}

		insertSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES(%s)",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholder, ", "),
		)
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to prepare statement, %v", err)
		}
		defer stmt.Close()

		//Inserting rows
		for _, row := range rows {
			values := make([]interface{}, len(columns))
			for i, col := range columns {
				values[i] = row[col]
			}
			_, err := stmt.Exec(values...)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to insert row, %v", err)
			}
		}
		//Commit transaction
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction, %v", err)
		}
		fmt.Printf("Successfully imported %d rows into table %s", len(rows), tableName)
	}
	return nil
}

// SQLParser provides methods for parsingSQL files
type SQLParser struct{}

// Extracts table names from the SQL file content
func (p *SQLParser) ExtractTableNames(content string) ([]string, error) {
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + `(?:[\"\[']?(\w+)[\"\]']?\.)?[\"\[']?(\w+)[\"\]']?`)

	matches := re.FindAllStringSubmatch(content, -1)
	var tableNames []string

	for _, match := range matches {
		if len(match) > 2 && match[1] != "" {
			//Schema qualified table
			tableNames = append(tableNames, match[1]+"."+match[2])
		} else if len(match) > 2 {
			//Just table name
			tableNames = append(tableNames, match[2])
		}
	}
	return tableNames, nil
}

// Read the SQL file to get tablenames
func (p *SQLParser) ParseSQLFiles(filepath string) ([]string, error) {
	content, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read the SQL file, %v", err)
	}
	return p.ExtractTableNames(string(content))
}

// this helps in preventing SQL injection by sanitizing identifiers(to avoid malicious drops for eg)
func sanitizeIdentifier(identifier string) string {
	return strings.Replace(identifier, "'", "", -1)
}

//Backward Compatible functions

func ConnectMySQL(user, password, host string, port int, dbname string) (*sql.DB, error) {
	client := NewMySQLClient(user, password, host, port, dbname)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("could not connect to the SQL Database, %v", err)
	}
	fmt.Println("successfully connected to the MySQL database...")
	return client.DB, nil
}

func ConnectMySQLFromConfig(cfg *config.Config) (*sql.DB, error) {
	client := NewMYSQLClientFromConfig(cfg)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("could not connect to the MySQL dtabase")
	}
	fmt.Println("Successfully connected to the MySQL Database")
	return client.DB, nil
}

func ExtractTableNamesFromSQLFile(filepath string) ([]string, error) {
	parser := &SQLParser{}
	return parser.ParseSQLFiles(filepath)
}

func FetchData(db *sql.DB, sqlFilepath string) ([]map[string]interface{}, error) {
	//create a temporary client with the provided DB connection
	client := &MySQLClient{DB: db}

	//Parse the SQL file
	parser := &SQLParser{}
	tableNames, err := parser.ParseSQLFiles(sqlFilepath)
	if err != nil {
		return nil, fmt.Errorf("failed to extract table names, %v", err)
	}

	if len(tableNames) == 0 {
		return nil, fmt.Errorf("no tables found in the SQL file")
	}
	//fetch data from all tables
	return client.FetchAllData(tableNames)
}

func FetchDataFromConfig(cfg *config.Config) ([]map[string]interface{}, error) {
	// Create client from config and connect
	client := NewMYSQLClientFromConfig(cfg)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer client.Close()

	// Parse the SQL file from config
	parser := &SQLParser{}
	tableNames, err := parser.ParseSQLFiles(cfg.SQLFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to extract table names: %w", err)
	}

	if len(tableNames) == 0 {
		return nil, fmt.Errorf("no tables found in the SQL file")
	}

	// Fetch data from all tables
	return client.FetchAllData(tableNames)
}

// Helper function  for MYSQL create table
func generateMySQLCreateTableSQL(tableName string, sampleRow map[string]interface{}) string {
	columns := make([]string, 0, len(sampleRow)-1)
	for col, val := range sampleRow {
		if col == "_source_table" {
			continue
		}
		//Determining MySQL data type GO datatypes
		var dataType string
		switch val.(type) {
		case int, int32, int64:
			dataType = "INT"
		case float32, float64:
			dataType = "DECIMAL(10,2)"
		case bool:
			dataType = "BOOLEAN"
		case string:
			dataType = "TEXT"
		case []byte:
			dataType = "BLOB"
		case nil:
			dataType = "TEXT"
		default:
			dataType = "TEXT"
		}
		columns = append(columns, fmt.Sprintf("%s %s", col, dataType))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columns, ", "))
}
