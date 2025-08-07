package database

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/SusheelSathyaraj/DataMigrationTool/config"
	_ "github.com/lib/pq"
)

type PostgreSQLClient struct {
	User     string
	Password string
	Host     string
	Port     int
	DBName   string
	DB       *sql.DB
}

func NewPostgreSQLClient(user, password, host string, port int, dbname string) *PostgreSQLClient {
	return &PostgreSQLClient{
		User:     user,
		Password: password,
		Host:     host,
		Port:     port,
		DBName:   dbname,
	}
}

func NewPostgreSQLClientFromConfig(cfg *config.Config) *PostgreSQLClient {
	return &PostgreSQLClient{
		User:     cfg.PostgreSQL.User,
		Password: cfg.PostgreSQL.Password,
		Host:     cfg.PostgreSQL.Host,
		Port:     cfg.PostgreSQL.Port,
		DBName:   cfg.PostgreSQL.DBName,
	}
}

// connect to Postgresql database
func (p *PostgreSQLClient) Connect() error {
	//DSN for postgresql
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", p.Host, p.Port, p.User, p.Password, p.DBName)

	//open connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open Postgresql connection,%v", err)
	}

	//testing connection
	if err = db.Ping(); err != nil {
		return fmt.Errorf("failed to ping postgresql database,%v", err)
	}
	p.DB = db
	fmt.Println("successfully connected to the postgresql database!!!")
	return nil
}

// Close the database connection
func (p *PostgreSQLClient) Close() error {
	if p.DB != nil {
		return p.DB.Close()
	}
	return nil
}

// Executing a query
func (p *PostgreSQLClient) ExecuteQuery(query string) (*sql.Rows, error) {
	if p.DB == nil {
		return nil, fmt.Errorf("database connection not established")
	}
	return p.DB.Query(query)
}

func (p *PostgreSQLClient) FetchAllData(tables []string) ([]map[string]interface{}, error) {
	if p.DB == nil {
		return nil, fmt.Errorf("database connection not established")
	}
	var allResults []map[string]interface{}

	for _, tableName := range tables {
		//sanitise table name to prevent SQL injection
		sanitizedTableName := sanitizeIdentifier(tableName)
		query := fmt.Sprintf("SELECT * FROM %s;", sanitizedTableName)

		rows, err := p.DB.Query(query)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query on table %s, %v", tableName, err)
		}
		defer rows.Close()

		//Get column names
		columns, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("failed to get column names for table %s, %v", tableName, err)
		}
		//iterate through rows
		for rows.Next() {
			//Create a slice of interface{} to hold values
			values := make([]interface{}, len(columns))
			valuesptr := make([]interface{}, len(columns))

			//setup pointers
			for i := range values {
				valuesptr[i] = &values[i]
			}

			//scan the rows into pointers
			if err := rows.Scan(valuesptr...); err != nil {
				return nil, fmt.Errorf("failed to scam row, %v", err)
			}

			//Create a map for the row
			rowMap := make(map[string]interface{})
			rowMap["_source_table"] = tableName

			//convert []byte to string
			for i, colName := range columns {
				val := values[i]
				if b, okay := val.([]byte); okay {
					rowMap[colName] = string(b)
				} else {
					rowMap[colName] = val
				}
			}
			allResults = append(allResults, rowMap)
		}
		//check for errors after iterating through rows
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error during row iteration %v", err)
		}
	}
	return allResults, nil
}

// fecthes data from mulitple tables using workerpool
func (p *PostgreSQLClient) FetchAllDataConcurrently(tables []string, numWorkers int) ([]map[string]interface{}, error) {
	if numWorkers <= 0 {
		numWorkers = 4 //Default number of workers
	}
	return ProcessTablesWithWorkerPool(p, tables, numWorkers)
}

func (p *PostgreSQLClient) ImportData(data []map[string]interface{}) error {
	if p.DB == nil {
		return fmt.Errorf("database connection not established")
	}
	if len(data) == 0 {
		return fmt.Errorf("no data to import")
	}

	//Grouping data by table
	tableData := make(map[string][]map[string]interface{})
	for _, row := range data {
		tableName, ok := row["_source_table"].(string)
		if !ok {
			return fmt.Errorf("row missing source table information")
		}
		tableData[tableName] = append(tableData[tableName], row)
	}
	//Process each table
	for tableName, rows := range tableData {
		if len(rows) == 0 {
			continue
		}
		//get column names except _source_table
		first_row := rows[0]
		columns := make([]string, 0, len(first_row)-1)
		for col := range first_row {
			if col != "_source_table" {
				columns = append(columns, col)
			}
		}

		//Begin migration
		tx, err := p.DB.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transation,%v", err)
		}

		//Creating table if not present
		createTableSQL := generateCreateTableSQL(tableName, first_row)
		_, err = tx.Exec(createTableSQL)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to create table %s, %v", tableName, err)
		}

		//Prepare insert statement
		placeholder := make([]string, len(columns))
		for i := range placeholder {
			placeholder[i] = fmt.Sprintf("$%d", i+1)
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

		//Insert row
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
		fmt.Printf("Successfully imported %d rows into table %s \n", len(rows), tableName)
	}
	return nil
}

// imports data uing batch processing
func (p *PostgreSQLClient) ImportDataConcurrently(data []map[string]interface{}, batchsize int) error {
	if batchsize <= 0 {
		batchsize = 1000 //default size of the batch
	}
	processor := NewBatchProcessor(batchsize)

	return processor.ProcessInBatches(data, p.ImportData)
}

// Helper function
func generateCreateTableSQL(tableName string, sampleRow map[string]interface{}) string {
	columns := make([]string, 0, len(sampleRow)-1)
	for col, val := range sampleRow {
		if col == "_source_table" {
			continue
		}

		//Determine postgresql datatype based on Go type
		var dataType string
		switch val.(type) {
		case int, int32, int64:
			dataType = "INTEGER"
		case float32, float64:
			dataType = "NUMERIC"
		case bool:
			dataType = "BOOLEAN"
		case string:
			dataType = "TEXT"
		case []byte:
			dataType = "BYTE"
		case nil:
			dataType = "TEXT"
		default:
			dataType = "TEXT"
		}
		columns = append(columns, fmt.Sprintf("%s %s", col, dataType))
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);",
		tableName, strings.Join(columns, ", "))
}

// Adding PostgreSQL parsing
func (p *PostgreSQLClient) ExtractTableNames(content string) ([]string, error) {
	//regex handling schema tables
	re := strings.NewReplacer(
		"create table", "CREATE TABLE",
		"CREATE table", "CREATE TABLE",
		"create TABLE", "CREATE TABLE",
	)
	normalisedContent := re.Replace(content)

	//Extract table names
	tables := make([]string, 0)
	lines := strings.Split(normalisedContent, ";")

	for _, line := range lines {
		if strings.Contains(line, "CREATE TABLE") {
			parts := strings.Split(line, "CREATE TABLE")
			if len(parts) > 1 {
				tablePart := strings.TrimSpace(parts[1])
				tableName := strings.Split(tablePart, "")[0]
				tableName = strings.Trim(tableName, `"()`)
				if tableName != "" {
					tables = append(tables, tableName)
				}
			}
		}
	}
	return tables, nil
}

// backward compatibility test
func ConnectPostgres(cfg config.PostgreSQLConfig) (*sql.DB, error) {
	client := NewPostgreSQLClient(cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("could not connect to the PostGres database, %v", err)
	}
	return client.DB, nil
}

func ExtractTableNamesFromPostgreSQLFile(filepath string) ([]string, error) {
	parser := &SQLParser{}
	return parser.ParseSQLFiles(filepath)
}

func FetchDataFromPostGreSQL(db *sql.DB, sqlFilepath string) ([]map[string]interface{}, error) {
	// Create a temporary client with the provided DB connection
	client := &PostgreSQLClient{DB: db}

	//Parse the SQL file
	parser := &SQLParser{}
	tableNames, err := parser.ParseSQLFiles(sqlFilepath)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tablenames, %v", err)
	}

	if len(tableNames) == 0 {
		return nil, fmt.Errorf("no tables found in the SQL file")
	}

	//fetch data from all tables
	return client.FetchAllData(tableNames)
}
