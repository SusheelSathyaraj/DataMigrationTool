package database

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

func TestMySQLConnection(t *testing.T) {
	//Ensure environment variables are all fetched

	dbuser := os.Getenv("MYSQL_USER")
	dbpass := os.Getenv("MYSQL_PASS")
	dbname := os.Getenv("MYSQL_NAME")
	dbhost := os.Getenv("MYSQL_HOST")
	dbport := os.Getenv("MYSQL_PORT")

	//if any env variable is missing, skip test
	if dbuser == "" || dbpass == "" || dbname == "" || dbhost == "" || dbport == "" {
		t.Skip("Skipping Tests: All of the Environment Variables must be present")
	}

	//Creating DSN for MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbuser, dbpass, dbhost, dbport, dbname)

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
