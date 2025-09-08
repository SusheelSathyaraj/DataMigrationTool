package test

import (
	"database/sql"
	"fmt"
	"time"
)

// struct for testing migration engine
type CompleteMockDatabaseClient struct {
	name         string
	connected    bool
	data         map[string][]map[string]interface{}
	importedData map[string][]map[string]interface{}

	failOnConnect bool
	failOnFetch   string
	failOnImport  bool
	fetchDelay    time.Duration
	importDelay   time.Duration

	connectCalled int
	closeCalled   int
	fetchCalled   int
	importCalled  int
	queryCalled   int
}

func NewCompleteMockDatabaseClient(name string) *CompleteMockDatabaseClient {
	return &CompleteMockDatabaseClient{
		name:         name,
		data:         make(map[string][]map[string]interface{}),
		importedData: make(map[string][]map[string]interface{}, 0),
		fetchDelay:   0,
		importDelay:  0,
	}
}

func (m *CompleteMockDatabaseClient) Connect() error {
	m.connectCalled++
	if m.failOnConnect {
		return fmt.Errorf("mock connection failure for %s", m.name)
	}
	//simulating connection time
	if m.fetchDelay > 0 {
		time.Sleep(m.fetchDelay)
	}
	m.connected = true
	return nil
}

func (m *CompleteMockDatabaseClient) Close() error {
	m.closeCalled++
	m.connected = false
	return nil
}

func (m *CompleteMockDatabaseClient) ExecuteQuery(query string) (*sql.Rows, error) {
	m.queryCalled++

	if !m.connected {
		return nil, fmt.Errorf("database %s not connected", m.name)
	}
	//this would be *sql.rows in real world scenario
	return nil, nil
}

func (m *CompleteMockDatabaseClient) FetchAllData(tables []string) ([]map[string]interface{}, error) {
	m.fetchCalled++

	if m.failOnFetch != "" {
		for _, table := range tables {
			if table == m.failOnFetch {
				return nil, fmt.Errorf("mock fetch error for tables %s", table)
			}
		}
	}

	//simulate fetch delay
	if m.fetchDelay > 0 {
		time.Sleep(m.fetchDelay)
	}

	var allData []map[string]interface{}
	for _, table := range tables {
		if data, exists := m.data[table]; exists {
			for _, row := range data {
				//creating a copy of to avoid modifying original data
				rowCopy := make(map[string]interface{})
				for k, v := range row {
					rowCopy[k] = v
				}
				rowCopy["_source_table"] = table
				allData = append(allData, rowCopy)
			}
		}
	}
	return allData, nil
}

func (m *CompleteMockDatabaseClient) FetchAllDataConcurrently(tables []string, numWorkers int) ([]map[string]interface{}, error) {
	//simulating concurrent processing with slight delay
	originalDelay := m.fetchDelay
	if m.fetchDelay > 0 {
		m.fetchDelay = m.fetchDelay / 2 //simulating speedup from concurrency
	}

	result, err := m.FetchAllData(tables)
	m.fetchDelay = originalDelay //restore original delay
	return result, err
}

func (m *CompleteMockDatabaseClient) ImportData(data []map[string]interface{}) error {
	m.importCalled++

	if !m.connected {
		return fmt.Errorf("database %s not connected", m.name)
	}

	if m.failOnImport {
		return fmt.Errorf("mock import err failed for %s", m.name)
	}

	//simulating import delay
	if m.importDelay > 0 {
		time.Sleep(m.importDelay)
	}

	for _, row := range data {
		if tableNameInterface, exists := row["_source_table"]; exists {
			tableName := tableNameInterface.(string)

			if m.importedData[tableName] == nil {
				m.importedData[tableName] = make([]map[string]interface{}, 0)
			}

			//storing  clean row without metadata
			cleanRow := make(map[string]interface{})
			for k, v := range row {
				if k != "_source_table" {
					cleanRow[k] = v
				}
			}
			m.importedData[tableName] = append(m.importedData[tableName], cleanRow)
		}
	}

	return nil
}

func (m *CompleteMockDatabaseClient) ImportDataConcurrently(data []map[string]interface{}, batchSize int) error {
	if batchSize <= 0 {
		return m.ImportData(data)
	}

	//processing in batches
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		if err := m.ImportData(batch); err != nil {
			return fmt.Errorf("batch import failed at position %d, %v", i, err)
		}
		//simulating batch processing delay
		if m.importDelay > 0 {
			time.Sleep(m.importDelay / 10) //smaller delay per batch
		}
	}
	return nil
}

//Helper methods for test coverage

//adding test data to the mock database

func (m *CompleteMockDatabaseClient) AddTestData(table string, data []map[string]interface{}) {
	m.data[table] = make([]map[string]interface{}, len(data))
	copy(m.data[table], data)
}

func (m *CompleteMockDatabaseClient) GetImportedData(table string) []map[string]interface{} {
	if data, exists := m.importedData[table]; exists {
		return data
	}
	return []map[string]interface{}{}
}

func (m *CompleteMockDatabaseClient) GetAllImporetedData() map[string][]map[string]interface{} {
	return m.importedData
}

func (m *CompleteMockDatabaseClient) GetTotalImportedRows() int {
	total := 0
	for _, tableData := range m.importedData {
		total += len(tableData)
	}
	return total
}

func (m *CompleteMockDatabaseClient) SetFailOnConnect(fail bool) {
	m.failOnConnect = fail
}

func (m *CompleteMockDatabaseClient) SetFailOnFetch(table string) {
	m.failOnFetch = table
}

func (m CompleteMockDatabaseClient) SetFailOnImport(fail bool) {
	m.failOnImport = fail
}

func (m *CompleteMockDatabaseClient) SetFetchDelay(delay time.Duration) {
	m.fetchDelay = delay
}

func (m *CompleteMockDatabaseClient) SetImportDelay(delay time.Duration) {
	m.importDelay = delay
}

// call count methods
func (m *CompleteMockDatabaseClient) GetConnectCallCoutn() int {
	return m.connectCalled
}

func (m *CompleteMockDatabaseClient) GetCloseCallCount() int {
	return m.closeCalled
}

func (m *CompleteMockDatabaseClient) GetFetchCallCount() int {
	return m.fetchCalled
}

func (m *CompleteMockDatabaseClient) GetImportCallCount() int {
	return m.importCalled
}

func (m *CompleteMockDatabaseClient) GetQueryCallCount() int {
	return m.queryCalled
}

// state verification methods
func (m *CompleteMockDatabaseClient) IsConnected() bool {
	return m.connected
}

func (m *CompleteMockDatabaseClient) GetName() string {
	return m.name
}

func (m *CompleteMockDatabaseClient) HasTable(table string) bool {
	_, exists := m.data[table]
	return exists
}

func (m *CompleteMockDatabaseClient) GetTableRowCount(table string) int {
	if data, exists := m.data[table]; exists {
		return len(data)
	}
	return 0
}

func (m *CompleteMockDatabaseClient) GetImportedTableRowCount(table string) int {
	if data, exists := m.importedData[table]; exists {
		return len(data)
	}
	return 0
}

// reset the mock client to initial state
func (m *CompleteMockDatabaseClient) Reset() {
	m.connected = false
	m.data = make(map[string][]map[string]interface{})
	m.importedData = make(map[string][]map[string]interface{})
	m.failOnConnect = false
	m.failOnFetch = ""
	m.fetchDelay = 0
	m.importDelay = 0
	m.connectCalled = 0
	m.closeCalled = 0
	m.fetchCalled = 0
	m.importCalled = 0
	m.queryCalled = 0
}

// simulating realistic database behaviour for advanced testing
func (m *CompleteMockDatabaseClient) SimulateConnectionIssue() {
	m.connected = false
}

func (m *CompleteMockDatabaseClient) SimulateSlowConnection() {
	m.fetchDelay = 100 * time.Millisecond
	m.importDelay = 100 * time.Millisecond
}

func (m *CompleteMockDatabaseClient) SimulateFastConnection() {
	m.fetchDelay = 1 * time.Millisecond
	m.importDelay = 1 * time.Millisecond
}
