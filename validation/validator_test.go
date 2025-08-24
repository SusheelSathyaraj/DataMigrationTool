package validation

import (
	"database/sql"
	"errors"
)

// mockdatabase for testing
type MockDatabaseClient struct {
	mockData map[string][]map[string]interface{}
	failOn   string //table name to fail on
}

func (m *MockDatabaseClient) Connect() error                                 { return nil }
func (m *MockDatabaseClient) Close() error                                   { return nil }
func (m *MockDatabaseClient) ExecuteQuery(query string) (*sql.Rows, error)   { return nil, nil }
func (m *MockDatabaseClient) ImportData(data []map[string]interface{}) error { return nil }
func (m *MockDatabaseClient) FetchAllDataConcurrently(tables []string, numWorkers int) ([]map[string]interface{}, error) {
	return m.FetchAllData(tables)
}
func (m *MockDatabaseClient) ImportDataConcurrently(data []map[string]interface{}, batchSize int) error {
	return m.ImportData(data)
}

func (m *MockDatabaseClient) FetchAllData(tables []string) ([]map[string]interface{}, error) {
	if len(tables) == 0 {
		return []map[string]interface{}{}, nil
	}

	var allData []map[string]interface{}
	for _, table := range tables {
		if table == m.failOn {
			return nil, errors.New("mock error for table" + table)
		}

		if data, exists := m.mockData[table]; exists {
			//add source table metadata
			for i := range data {
				data[i]["_source_table"] = table
			}
			allData = append(allData, data...)
		}
	}
	return allData, nil
}

func NewMockDatabaseClient() *MockDatabaseClient {
	return &MockDatabaseClient{
		mockData: make(map[string][]map[string]interface{}),
	}
}

func (m *MockDatabaseClient) AddMockData(table string, data []map[string]interface{}) {
	m.mockData[table] = data
}

func (m *MockDatabaseClient) SetFailOn(table string) {
	m.failOn = table
}
