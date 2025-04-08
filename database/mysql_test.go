package database

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SusheelSathyaraj/DataMigrationTool/config"
	_ "github.com/go-sql-driver/mysql"
)

var cfg *config.Config

func TestMain(m *testing.M) {
	//Loading configuration
	var err error
	cfg, err = config.LoadConfig("../config.yaml")
	if err != nil {
		log.Fatalf("Error loading the config file, %v", err)
	}
	//run tests
	os.Exit(m.Run())
}

func TestMySQLConnection(t *testing.T) {
	//Get creds from the config file
	dbuser := cfg.Database.User
	dbpass := cfg.Database.Password
	dbname := cfg.Database.DBName
	dbhost := cfg.Database.Host
	dbport := cfg.Database.Port

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

	//simulate dynamic schema
	columnName := []string{"col1", "col2", "col3", "col4"} //simulate arbitrary columns
	mockRows := sqlmock.NewRows(columnName).AddRow(1, "Alice", 25, 50000).AddRow(2, "Alex", 26, 65000).AddRow(3, "Susheel", 37, 100000).AddRow(4, "Fahad", 36, 150000)

	mock.ExpectQuery("SELECT \\* FROM .*;").WillReturnRows(mockRows) //returns from all tables, generic query

	//call fetchdata func
	data, err := FetchData(db, cfg.FilePath)

	//Assertions
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	if len(data) != 2 {
		t.Errorf("expected 2 rows, got %d", len(data))
	}
}

func TestMySQLFetchData_EmptyTable(t *testing.T) {
	//testing when no rows exist
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating mock database %v", err)
	}
	defer db.Close()

	//simulate dynamic schema
	columnName := []string{"col1", "col2", "col3", "col4"}
	mockRows := sqlmock.NewRows(columnName)

	//query execution
	mock.ExpectQuery("SELECT //* FROM .*").WillReturnRows(mockRows)

	data, err := FetchData(db, cfg.FilePath)
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
	mock.ExpectQuery("SELECT //* FROM .*").WillReturnError(errors.New("query failed!!"))

	//calling fetchdata func
	data, err := FetchData(db, cfg.FilePath)
	if err == nil {
		t.Errorf("expected error, got nil")
	}

	if len(data) != 0 {
		t.Errorf("expected 0 rows, but got %d", len(data))
	}
}
