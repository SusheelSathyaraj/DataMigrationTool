package database

import "database/sql"

// Interface for database operations
type DatabaseClient interface {
	Connect() error
	Close() error
	FetchAllData(tables []string) ([]map[string]interface{}, error)
	ExecuteQuery(query string) (*sql.Rows, error)
	ImportData(data []map[string]interface{}) error
	FetchAllDataConcurrently(tables []string, numWorkers int) ([]map[string]interface{}, error)
	ImportDataConcurrently(data []map[string]interface{}, batchsize int) error
}

type TargetDatabase interface {
	Connect() error
	InsertData(data []map[string]interface{}) error
}
