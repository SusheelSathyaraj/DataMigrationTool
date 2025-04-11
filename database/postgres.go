package database

import (
	"database/sql"
	"fmt"

	"github.com/SusheelSathyaraj/DataMigrationTool/config"
)

type PostgresMigration struct {
}

func ConnectPostgres(cfg config.PostgreSQLConfig) (*sql.DB, error) {
	//Format: "host=localhost port=5432 user=postgres password=mysecretpassword dbname=mydb sslmode=disable"
	dsn := fmt.Sprintf("host=%s port=%d user =%s password =%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgresql connection,%v", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres, %v", err)
	}
	fmt.Println("Successfully connected to PostgreSQL database...!")
	return db, nil
}

func (p *PostgresMigration) InsertData(data []map[string]interface{}) error {
	return nil
}
