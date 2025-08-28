package test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/database"
	"github.com/SusheelSathyaraj/DataMigrationTool/migration"
)

// struct to run end to end migration
type IntegrationTestSuite struct {
	sourceClient database.DatabaseClient
	targetClient database.DatabaseClient
}

// struct for mock
type MockDatabaseForIntegration struct {
	name         string
	data         map[string][]map[string]interface{}
	importedData map[string][]map[string]interface{}
	connected    bool
}

func NewMockDatabaseForIntegration(name string) *MockDatabaseForIntegration {
	return &MockDatabaseForIntegration{
		name:         name,
		data:         make(map[string][]map[string]interface{}),
		importedData: make(map[string][]map[string]interface{}),
	}
}

func (m *MockDatabaseForIntegration) Connect() error {
	//simulating connection delay
	time.Sleep(100 * time.Millisecond)
	m.connected = true
	return nil
}

func (m *MockDatabaseForIntegration) Close() error {
	m.connected = false
	return nil
}

func (m *MockDatabaseForIntegration) ExecuteQuery(query string) (*sql.Rows, error) {
	return nil, nil
}

func (m *MockDatabaseForIntegration) FetchAllData(tables []string) ([]map[string]interface{}, error) {
	if !m.connected {
		return nil, fmt.Errorf("database %s not connected", m.name)
	}

	var allData []map[string]interface{}
	for _, table := range tables {
		if tableData, exists := m.data[table]; exists {
			for _, row := range tableData {
				rowCopy := make(map[string]interface{})
				for k, v := range row {
					rowCopy[k] = v
				}
				rowCopy["_source_table"] = table
				allData = append(allData, rowCopy)
			}
		}
	}
	//simulate fetch delay for realistic testing
	time.Sleep(50 * time.Millisecond)
	return allData, nil
}

func (m *MockDatabaseForIntegration) FetchAllDataConcurrently(tables []string, numWorkers int) ([]map[string]interface{}, error) {
	//simulating concurrent processing by adding a delay
	time.Sleep(25 * time.Millisecond)
	return m.FetchAllData(tables)
}

func (m *MockDatabaseForIntegration) ImportData(data []map[string]interface{}) error {
	if !m.connected {
		return fmt.Errorf("database %s not connected", m.name)
	}
	//groupping by table
	for _, row := range data {
		tableName := row["_source_table"].(string)
		if m.importedData[tableName] == nil {
			m.importedData[tableName] = make([]map[string]interface{}, 0)
		}

		//removing metadata before starting
		cleanRow := make(map[string]interface{})
		for k, v := range row {
			if k != "_source_table" {
				cleanRow[k] = v
			}
		}
		m.importedData[tableName] = append(m.importedData[tableName], cleanRow)
	}
	//simulate import delay
	time.Sleep(30 * time.Millisecond)
	return nil
}

func (m *MockDatabaseForIntegration) ImportDataConcurrently(data []map[string]interface{}, batchSize int) error {
	//simulating batch processing
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		if err := m.ImportData(batch); err != nil {
			return err
		}
		//simlutae batch processing delay
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (m *MockDatabaseForIntegration) AddTestData(table string, data []map[string]interface{}) {
	m.data[table] = data
}

func (m *MockDatabaseForIntegration) GetImportedData(table string) []map[string]interface{} {
	return m.importedData[table]
}

func (m *MockDatabaseForIntegration) GetAllImportedData() map[string][]map[string]interface{} {
	return m.importedData
}

func TestFullMigrationIntegration(t *testing.T) {
	sourceDB := NewMockDatabaseForIntegration("mysql")
	targetDB := NewMockDatabaseForIntegration("postgresql")

	//adding test data
	usersData := []map[string]interface{}{
		{"id": 1, "username": "Susheel", "email": "susheel@example.com", "created_at": "2025-11-28 10:00:00"},
		{"id": 2, "username": "Sathyaraj", "email": "sathyaraj@example.com", "created_at": "2025-11-29 10:30:00"},
		{"id": 3, "username": "SusheelSathyaraj", "email": "susheelsathyraj@example.com", "created_at": "2025-11-30 11:00:00"},
	}

	ordersData := []map[string]interface{}{
		{"id": 1, "user_id": 1, "product": "laptop", "amount": 11999.99, "order_date": "2025-08-11"},
		{"id": 2, "user_id": 2, "product": "monitor", "amount": 1009, "order_date": "2025-07-14"},
		{"id": 3, "user_id": 1, "product": "mouse", "amount": 99.99, "order_date": "2025-07-14"},
		{"id": 4, "user_id": 3, "product": "keyboard", "amount": 109.99, "order_date": "2025-08-11"},
	}

	sourceDB.AddTestData("users", usersData)
	targetDB.AddTestData("orders", ordersData)

	//creating migration configuration
	config := migration.MigrationConfig{
		Mode:         migration.FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users", "orders"},
		Workers:      2,
		BatchSize:    2,
		Concurrent:   true,
		ValidateData: true,
	}

	if err := sourceDB.Connect(); err != nil {
		t.Fatalf("Failed to connect to source database,%v", err)
	}
	defer sourceDB.Close()

	if err := targetDB.Connect(); err != nil {
		t.Fatalf("Failed to connect to target database, %v ", err)
	}
	defer targetDB.Close()

	engine := migration.NewMigrationEngine(config, sourceDB, targetDB)
	result, err := engine.ExecuteMigration()
	if err != nil {
		t.Fatalf("Migration Failed, %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful migration, got failure")
		if len(result.Errors) > 0 {
			t.Errorf("Migration errors, %v", result.Errors)
		}
	}

	//verifying data integrity
	if result.TotalRowsMigrated != 7 { //3users+4orders
		t.Errorf("Expected 7 rows to be migrated, got %d", result.TotalRowsMigrated)
	}

	if result.TotalTablesProcessed != 2 {
		t.Errorf("Expected 2 tables to be processed, got %d ", result.TotalTablesProcessed)
	}

	//verifying imported data
	importedUsers := targetDB.GetImportedData("users")
	importedOrders := targetDB.GetImportedData("orders")

	if len(importedUsers) != 3 {
		t.Errorf("Expected 3 users to be imported, got %d", len(importedUsers))
	}

	if len(importedOrders) != 4 {
		t.Errorf("Expected 4 orders to be imported, got %d", len(importedOrders))
	}

	//veriying specific data points
	if importedUsers[0]["username"] != "Susheel" {
		t.Errorf("Expected the forst user to be Susheel, got %s", importedUsers[0]["username"])
	}

	if importedOrders[3]["product"] != "keyboard" {
		t.Errorf("Expected the 4th product to be keyboard, got %s", importedOrders[3]["product"])
	}

	// verifying migration timing
	if result.Duration == 0 {
		t.Errorf("Expected migration duration >0, got %v", result.Duration)
	}

	t.Logf("Migration Completed successfully")
	t.Logf("Duration: %v", result.Duration)
	t.Logf("Rows: %d", result.TotalRowsMigrated)
	t.Logf("Tables: %d", result.TotalTablesProcessed)
}
