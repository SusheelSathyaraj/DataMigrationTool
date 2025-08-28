package test

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/database"
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
