package main

type Migration interface {
	Connect() error
	FetchData() ([]map[string]interface{}, error)
	MigrateData() ([]map[string]interface{}, error)
}
