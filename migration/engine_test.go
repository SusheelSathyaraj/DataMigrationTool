package migration

import (
	"database/sql"
	"fmt"
	"testing"
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

func NewCompleteCompleteMockDatabaseClient(name string) *CompleteMockDatabaseClient {
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
	return m.ImportData(data)
}

func (m *CompleteMockDatabaseClient) AddMockData(table string, data []map[string]interface{}) {
	m.mockData[table] = data
}

func (m *CompleteMockDatabaseClient) SetFailOnFetch(table string) {
	m.failOnFetch = table
}

func (m CompleteMockDatabaseClient) SetFailOnImport(fail bool) {
	m.failOnImport = fail
}

func (m *CompleteMockDatabaseClient) GetImportedData() []map[string]interface{} {
	return m.importedData
}

func TestMigrationEngineFullMigration(t *testing.T) {
	sourceClient := NewCompleteMockDatabaseClient()
	targetClient := NewCompleteMockDatabaseClient()

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
	sourceClient := NewCompleteMockDatabaseClient()
	targerClient := NewCompleteMockDatabaseClient()

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
	sourceClient := NewCompleteMockDatabaseClient()
	targetClient := NewCompleteMockDatabaseClient()

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
	sourceClient := NewCompleteMockDatabaseClient()
	targetClient := NewCompleteMockDatabaseClient()

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

func TestMigrationEngineWithConcurrentProcessing(t *testing.T) {
	sourceClient := NewCompleteMockDatabaseClient()
	targetClient := NewCompleteMockDatabaseClient()

	testData := []map[string]interface{}{
		{"id": 1, "name": "Susheel"},
		{"id": 2, "name": "Sathyaraj"},
	}
	sourceClient.AddMockData("users", testData)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		BatchSize:    1,
		Workers:      2,
		Concurrent:   true,
		ValidateData: false,
	}

	engine := NewMigrationEngine(config, sourceClient, targetClient)

	result, err := engine.ExecuteMigration()

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful migration, got failure")
	}

	importedData := targetClient.GetImportedData()
	if len(importedData) != 2 {
		t.Errorf("Expected 2 rows to be imported, got %d", len(importedData))
	}
}

func TestMigrationEngineIncrementalMode(t *testing.T) {
	sourceClient := NewCompleteMockDatabaseClient()
	targetCleint := NewCompleteMockDatabaseClient()

	config := MigrationConfig{
		Mode:         IncrementalMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		ValidateData: false,
	}

	engine := NewMigrationEngine(config, sourceClient, targetCleint)
	result, err := engine.ExecuteMigration()

	if err == nil {
		t.Errorf("Expected error as incremental migration is not implemented, got nil")
	}

	if result != nil && result.Success {
		t.Errorf("Expected migration failure due to unimplemented feature, got success")
	}
}

func TestMigrationEngineScheduledMode(t *testing.T) {
	sourceClient := NewCompleteMockDatabaseClient()
	targetClient := NewCompleteMockDatabaseClient()

	config := MigrationConfig{
		Mode:         ScheduledMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		ValidateData: false,
	}

	engine := NewMigrationEngine(config, sourceClient, targetClient)
	result, err := engine.ExecuteMigration()

	if err == nil {
		t.Errorf("Expected error as scheduled migration is not implemented, got nil")
	}

	if result != nil && result.Success {
		t.Errorf("Expected migration failure due to unimplemented feature, got success")
	}
}

func TestMigrationResultPrint(t *testing.T) {
	result := &MigrationResult{
		Success:              true,
		TotalTablesProcessed: 2,
		TotalRowsMigrated:    150,
		Duration:             5 * time.Minute,
		StartTime:            time.Now().Add(-5 * time.Minute),
		EndTime:              time.Now(),
		Errors:               []string{"Warning:Large table detected"},
	}

	//checks to ensure that print does not panic
	result.Print()
}

func TestMigrationConfigValidation(t *testing.T) {
	testCases := []struct {
		config      MigrationConfig
		expectError bool
		description string
	}{
		{
			config: MigrationConfig{
				Mode:     FullMigration,
				SourceDb: "mysql",
				TargetDb: "postgesql",
				Tables:   []string{"users"},
			},
			expectError: false,
			description: "Valid Full Migration config",
		},
		{
			config: MigrationConfig{
				Mode:     "invalid",
				SourceDb: "mysql",
				TargetDb: "postgresql",
				Tables:   []string{"users"},
			},
			expectError: true,
			description: "Invalid Migration Mode",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			sourceClient := NewCompleteMockDatabaseClient()
			targetCLient := NewCompleteMockDatabaseClient()

			if tc.config.Mode == FullMigration {
				//testdata for valid cases
				testData := []map[string]interface{}{
					{"id": 1, "name": "Susheel"},
				}
				sourceClient.AddMockData("users", testData)
			}
			engine := NewMigrationEngine(tc.config, sourceClient, targetCLient)
			_, err := engine.ExecuteMigration()

			if tc.expectError && err == nil {
				t.Errorf("Expected error for %s, got nil", tc.description)
			} else if !tc.expectError && err != nil {
				t.Errorf("Expected no error for %s, got %v", tc.description, err)
			}
		})
	}
}

func BenchmarkMigrationEngineFull(b *testing.B) {
	sourceClient := NewCompleteMockDatabaseClient()
	//	targetClient := NewCompleteMockDatabaseClient()

	//adding large test dataset
	var testData []map[string]interface{}
	for i := 0; i < 1000; i++ {
		testData = append(testData, map[string]interface{}{
			"id":   i,
			"name": fmt.Sprintf("User%d", i),
			"age":  25 + (i % 50),
		})
	}
	sourceClient.AddMockData("users", testData)
	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		ValidateData: false, //disabling as it is benchmark
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//resetting target client for each iteration
		targetClient := NewCompleteMockDatabaseClient()
		engine := NewMigrationEngine(config, sourceClient, targetClient)
		_, err := engine.ExecuteMigration()
		if err != nil {
			b.Fatal(err)
		}
	}
}
