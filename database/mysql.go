package database

import (
	"database/sql"
	"fmt"
	"os"
)

func ConnectMySQL() (*sql.DB, error) {
	fmt.Println("Connecting to the MySQL database")

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
