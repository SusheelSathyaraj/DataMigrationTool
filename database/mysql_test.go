package database

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SusheelSathyaraj/DataMigrationTool/config"
	_ "github.com/go-sql-driver/mysql"
)

var testConfig *config.Config

func TestMain(m *testing.M) {
	//Loading configuration
	configPath := filepath.Join("..", "config.yaml")
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Error loading the config file, %v", err)
		os.Exit(1)
	}
	//run tests

	testConfig = cfg
	os.Exit(m.Run())
}

func TestMySQLConnection(t *testing.T) {
	//Get creds from the config file
	dbuser := testConfig.Database.User
	dbpass := testConfig.Database.Password
	dbname := testConfig.Database.DBName
	dbhost := testConfig.Database.Host
	dbport := testConfig.Database.Port

	//if any env variable is missing, skip test
	if dbuser == "" || dbpass == "" || dbname == "" || dbhost == "" || dbport == 0 {
		t.Skip("Skipping Tests: All of the Environment Variables must be present")
	}

	//Creating DSN for MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbuser, dbpass, dbhost, dbport, dbname)

	// Attempting to open connection to MySQL
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to open to MySQL database %v", err)
	}
	defer db.Close()

	//check if we can successfully ping to the MySQL database
	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to connect to the MySQL database %v", err)
	}

	t.Log("Successfully connected to MySQL database")
}

// Test cases for FetchData method
func TestMySQLFetchData_Success(t *testing.T) {
	//successful execution of fetchdata
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating mock database %v", err)
	}
	defer db.Close()

	//loading a dynamic table name from the filepath
	tableNames, err := ExtractTableNamesFromSQLFile(testConfig.FilePath)
	if err != nil {
		t.Fatalf("Failed to extract the table names, %v", err)
	}
	if len(tableNames) == 0 {
		t.Fatalf("No table name found in the SQL file")
	}
	tableName := tableNames[0]

	//simulate dynamic schema
	columnName := []string{"col1", "col2", "col3", "col4"} //simulate arbitrary columns
	mockRows := sqlmock.NewRows(columnName).AddRow(1, "Alice", 25, 50000).AddRow(2, "Alex", 26, 65000).AddRow(3, "Susheel", 37, 100000).AddRow(4, "Fahad", 36, 150000)

	//Dynamically matching the expected query
	//query := fmt.Sprintf("SELECT \\* FROM %s;", tableName)
	query := fmt.Sprintf("(?i)^SELECT \\* FROM %s\\s*;?$", tableName)
	mock.ExpectQuery(query).WillReturnRows(mockRows) //returns from all tables, generic query

	//call fetchdata func
	data, err := FetchData(db, testConfig.FilePath)

	//Assertions
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	if len(data) != 4 {
		t.Errorf("expected 4 rows, got %d", len(data))
	}
}

func TestMySQLFetchData_EmptyTable(t *testing.T) {
	//testing when no rows exist
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating mock database %v", err)
	}
	defer db.Close()

	//dynamically extract table from the SQL file
	tableNames, err := ExtractTableNamesFromSQLFile(testConfig.FilePath)
	if err != nil {
		t.Fatalf("Failed to extract the table name from the SQL file, %v", err)
	}
	if len(tableNames) == 0 {
		t.Fatalf("There are no tables found in the SQL file")
	}
	tableName := tableNames[0]

	//simulate dynamic schema
	columnName := []string{"col1", "col2", "col3", "col4"}
	mockRows := sqlmock.NewRows(columnName)

	//using dynamically extract the table name for query execution
	query := fmt.Sprintf("(?i)^SELECT \\* FROM %s\\s*;?$", tableName)
	mock.ExpectQuery(query).WillReturnRows(mockRows)

	data, err := FetchData(db, testConfig.FilePath)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	if len(data) != 0 {
		t.Errorf("expected 0 rows, got %d", len(data))
	}
}

// Testing error scenarios
func TestMySQLFetchData_Error(t *testing.T) {
	//mock database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Errorf("error creating a mock database %v", err)
	}
	defer db.Close()

	//mock query failure
	mock.ExpectQuery("?(i)^SELECT \\* FROM %s\\s*;?$").WillReturnError(errors.New("query failed!!"))

	//calling fetchdata func
	data, err := FetchData(db, testConfig.FilePath)
	if err == nil {
		t.Errorf("expected error, got nil")
	}

	if len(data) != 0 {
		t.Errorf("expected 0 rows, but got %d", len(data))
	}
}
