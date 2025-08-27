package migration

import (
	"database/sql"
	"fmt"
	"testing"
)

// struct for testing migration engine
type MockDatabaseClient struct {
	mockData     map[string][]map[string]interface{}
	failOnFetch  string
	failOnImport bool
	importedData []map[string]interface{}
	fetchCalled  int
	importCalled int
}

func NewMockDatabaseClient() *MockDatabaseClient {
	return &MockDatabaseClient{
		mockData:     make(map[string][]map[string]interface{}),
		importedData: make([]map[string]interface{}, 0),
	}
}

func (m *MockDatabaseClient) Connect() error                               { return nil }
func (m *MockDatabaseClient) Close() error                                 { return nil }
func (m *MockDatabaseClient) ExecuteQuery(query string) (*sql.Rows, error) { return nil, nil }
func (m *MockDatabaseClient) FetchAllData(tables []string) ([]map[string]interface{}, error) {
	m.fetchCalled++

	if m.failOnFetch != "" {
		for _, table := range tables {
			if table == m.failOnFetch {
				return nil, fmt.Errorf("mock fetch error for tables %s", table)
			}
		}
	}

	var allData []map[string]interface{}
	for _, table := range tables {
		if data, exists := m.mockData[table]; exists {
			for _, row := range data {
				row["_source_table"] = table
				allData = append(allData, row)
			}
		}
	}
	return allData, nil
}

func (m *MockDatabaseClient) FetchAllDataConcurrently(tables []string, numWorkers int) ([]map[string]interface{}, error) {
	return m.FetchAllData(tables)
}

func (m *MockDatabaseClient) ImportData(data []map[string]interface{}) error {
	m.importCalled++

	if m.failOnImport {
		return fmt.Errorf("mock import err failed")
	}
	m.importedData = append(m.importedData, data...)
	return nil
}

func (m *MockDatabaseClient) ImportDataConcurrently(data []map[string]interface{}, batchSize int) error {
	return m.ImportData(data)
}

func (m *MockDatabaseClient) AddMockData(table string, data []map[string]interface{}) {
	m.mockData[table] = data
}

func (m *MockDatabaseClient) SetFailOnFetch(table string) {
	m.failOnFetch = table
}

func (m MockDatabaseClient) SetFailOnImport(fail bool) {
	m.failOnImport = fail
}

func (m *MockDatabaseClient) GetImportedData() []map[string]interface{} {
	return m.importedData
}

func TestMigrationEngineFullMigration(t *testing.T) {
	sourceClient := NewMockDatabaseClient()
	targetClient := NewMockDatabaseClient()

	//Adding test data
	testData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "age": 27},
		{"id": 1, "name": "Sathyaraj", "age": 28},
	}
	sourceClient.AddMockData("users", testData)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		Workers:      2,
		BatchSize:    1000,
		Concurrent:   false,
		ValidateData: true,
	}

	engine := NewMigrationEngine(config, sourceClient, targetClient)

	//execute migration
	result, err := engine.ExecuteMigration()

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	if !result.Success {
		t.Errorf("Expected successful migration, got failure")
	}

	if result.TotalRowsMigrated != 2 {
		t.Errorf("Expected 2 rows to be migrated, got %d", result.TotalRowsMigrated)
	}

	if result.TotalTablesProcessed != 1 {
		t.Errorf("Expected 1 table to be processed, got %d", result.TotalTablesProcessed)
	}

	//checking if data if imported
	importedData := targetClient.GetImportedData()
	if len(importedData) != 2 {
		t.Errorf("Expect 2 rows to be imported, got %d", len(importedData))
	}
}

func TestMigrationEngineWithFetchError(t *testing.T) {
	sourceClient := NewMockDatabaseClient()
	targerClient := NewMockDatabaseClient()

	//setting source to fail on fetch
	sourceClient.SetFailOnFetch("users")

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgesql",
		Tables:       []string{"users"},
		ValidateData: false,
	}
	engine := NewMigrationEngine(config, sourceClient, targerClient)

	result, err := engine.ExecuteMigration()

	if err == nil {
		t.Errorf("Expected error due to fetch failure, got nil")
	}

	if result == nil {
		t.Fatal("Expected result even on failure, got nil")
	}

	if result.Success {
		t.Errorf("Expected migration failure, got success")
	}

	if len(result.Errors) == 0 {
		t.Errorf("Expected errors in result, got none")
	}
}

func TestMigrationEngineWithImportError(t *testing.T) {
	sourceClient := NewMockDatabaseClient()
	targetClient := NewMockDatabaseClient()

	testData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "age": 30},
	}
	sourceClient.AddMockData("users", testData)

	targetClient.SetFailOnImport(true)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		ValidateData: false, //disabling validation as it is for testing import error
	}

	engine := NewMigrationEngine(config, sourceClient, targetClient)

	result, err := engine.ExecuteMigration()

	if err == nil {
		t.Errorf("Expected error due to import failure, got nil")
	}

	if result.Success {
		t.Errorf("Expected migration failure, got success")
	}
}

func TestMigrationEngineMultipleTables(t *testing.T) {
	sourceClient := NewMockDatabaseClient()
	targetClient := NewMockDatabaseClient()

	//add data for multiple tables
	usersData := []map[string]interface{}{
		{"id": 1, "name": "Susheel"},
		{"id": 2, "name": "Sathyaraj"},
	}

	ordersData := []map[string]interface{}{
		{"id": 1, "user_id": 1, "amount": 100.50},
		{"id": 2, "user_id": 2, "amount": 10},
		{"id": 3, "user_id": 1, "amount": 5070.50},
	}

	sourceClient.AddMockData("users", usersData)
	sourceClient.AddMockData("orders", ordersData)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users", "orders"},
		ValidateData: true,
	}

	engine := NewMigrationEngine(config, sourceClient, targetClient)

	result, err := engine.ExecuteMigration()

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful migration, got failure")
	}

	if result.TotalRowsMigrated != 5 {
		t.Errorf("Expected 5 rows to be migrated, got %d", result.TotalRowsMigrated)
	}

	if result.TotalTablesProcessed != 2 {
		t.Errorf("Expected 2 tables to be processed, got %d", result.TotalTablesProcessed)
	}

	//checking for import data
	importedData := targetClient.GetImportedData()

	if len(importedData) != 5 {
		t.Errorf("Expected 5 imported rows, found %d", len(importedData))
	}
}
