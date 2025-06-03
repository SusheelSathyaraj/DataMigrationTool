package database

type TargetDatabase interface {
	Connect() error
	InsertData(data []map[string]interface{}) error
}
