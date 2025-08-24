package validation

import (
	"database/sql"
	"errors"
	"math"
	"testing"
	"time"
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

func TestPreMigrationValidation(t *testing.T) {
	//test successful validation
	sourceClient := NewMockDatabaseClient()
	targetClient := NewMockDatabaseClient()

	//Add test data
	testData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "age": 27},
		{"id": 2, "name": "Sathyaraj", "age": 29},
	}
	sourceClient.AddMockData("users", testData)

	validator := NewMigrationValidator(sourceClient, targetClient)
	tables := []string{"users"}

	results, err := validator.PreMigrationValidation(tables)

	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if !results[0].IsValid {
		t.Errorf("Expected a valid result, got invalid %s", results[0].ErrorMessage)
	}

	if results[0].RowCount != 2 {
		t.Errorf("Expected the row count 2, got %d", results[0].RowCount)
	}
}

func TestPreMigrationValidationWithError(t *testing.T) {
	sourceClient := NewMockDatabaseClient()
	targetClient := NewMockDatabaseClient()

	//set up client to fail for specific table
	sourceClient.SetFailOn("users")

	validator := NewMigrationValidator(sourceClient, targetClient)
	tables := []string{"users"}

	results, err := validator.PreMigrationValidation(tables)

	if err != nil {
		t.Errorf("Premigration validator should not return an error, got %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].IsValid {
		t.Errorf("Expected invalid result, got valid")
	}
}

func TestPostMigrationValidation(t *testing.T) {
	sourceClient := NewMockDatabaseClient()
	targetClient := NewMockDatabaseClient()

	//add identical test data to both clients
	testData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "age": 29},
		{"id": 2, "name": "Sathyaraj", "age": 30},
	}
	sourceClient.AddMockData("users", testData)
	targetClient.AddMockData("users", testData)

	validator := NewMigrationValidator(sourceClient, targetClient)
	tables := []string{"users"}

	//get pre validation result first
	preResults, err := validator.PreMigrationValidation(tables)
	if err != nil {
		t.Fatalf("Pre-Validation failed, %v", err)
	}

	//run post validation
	postResults, err := validator.PostMigationValidation(tables, preResults)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if len(postResults) != 1 {
		t.Errorf("Expected 1 result, got %d", len(postResults))
	}

	if !postResults[0].IsValid {
		t.Errorf("Expected valid result, got invalid %s", postResults[0].ErrorMessage)
	}
}

func TestPostMigrationValidationRowCountMismatch(t *testing.T) {
	sourceClient := NewMockDatabaseClient()
	targetClient := NewMockDatabaseClient()

	//Add different amounts of data
	sourceData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "age": 31},
		{"id": 2, "name": "Sathyaraj", "age": 32},
	}
	targetData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "age": 31},
	}

	sourceClient.AddMockData("users", sourceData)
	targetClient.AddMockData("users", targetData)

	validator := NewMigrationValidator(sourceClient, targetClient)
	tables := []string{"users"}

	//get pre-validation result first
	preResults, err := validator.PreMigrationValidation(tables)
	if err != nil {
		t.Fatalf("Pre-validation failed, %v", err)
	}

	//run post-validation
	postResults, err := validator.PostMigationValidation(tables, preResults)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if len(postResults) != 1 {
		t.Errorf("Expected 1 result, got %d", len(postResults))
	}

	if postResults[0].IsValid {
		t.Errorf("Expected invalid result due to row count mismatch, got valid")
	}

	if postResults[0].ErrorMessage == "" {
		t.Errorf("Expected error message for row count mismatch")
	}
}

func TestValidateDataTypes(t *testing.T) {
	validator := &MigrationVaildator{}

	testData := []map[string]interface{}{
		{"id": 1,
			"name":          "Susheel",
			"salary":        150000.00,
			"is_active":     true,
			"_source_table": "users",
		},
	}
	err := validator.ValidateDataTypes(testData)
	if err != nil {
		t.Errorf("Expected no error for valid data types, got %v", err)
	}
}

func TestValidateDataTypesWithNaN(t *testing.T) {
	validator := &MigrationVaildator{}

	testData := []map[string]interface{}{
		{"id": 1,
			"invalid_float": float64(0) * math.Inf(1), //NaN
			"_source_table": "users",
		},
	}
	err := validator.ValidateDataTypes(testData)
	if err != nil {
		t.Errorf("Expected error for NaN value, got nil")
	}
}

func TestValidationSummary(t *testing.T) {
	startTime := time.Now()

	results := []ValidationResult{
		{TableName: "users", IsValid: true, RowCount: 100},
		{TableName: "orders", IsValid: true, RowCount: 250},
		{TableName: "products", IsValid: false, RowCount: 0, ErrorMessage: "table not found"},
	}

	summary := GenerateValidationSummary(results, startTime)

	if summary.TotalTables != 3 {
		t.Errorf("Expected 3 tables, got %d", summary.TotalRows)
	}

	if summary.ValidTables != 2 {
		t.Errorf("Expected 2 valid tables, got %d", summary.ValidTables)
	}

	if summary.InvalidTables != 1 {
		t.Errorf("Expected 1 invalide table, got %d", summary.InvalidTables)
	}

	if summary.TotalRows != 350 {
		t.Errorf("Expected 350 total no. of rows, got %d", summary.TotalRows)
	}

	if len(summary.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(summary.Errors))
	}
}

func TestCompareValues(t *testing.T) {
	testCases := []struct {
		v1       interface{}
		v2       interface{}
		expected bool
	}{
		{nil, nil, true},
		{nil, "something", false},
		{"hello", "world", true},
		{123, 123, true},
		{123, "123", true}, //string conversion should match
		{123.45, 123.45, true},
		{"hello", "world", false},
	}

	for i, tc := range testCases {
		result := compareValues(tc.v1, tc.v2)
		if result != tc.expected {
			t.Errorf("Test Case %d: CompareValues(%v,%v)=%v, expected %v", i+1, tc.v1, tc.v2, result, tc.expected)
		}
	}
}

func TestValidateSampleDataIntegrity(t *testing.T) {
	validator := &MigrationVaildator{}

	sourceData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "_source_table": "users"},
		{"id": 2, "name": "Sathyaraj", "_source_table": "users"},
	}

	targetData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "_source_table": "users"},
		{"id": 2, "name": "Sathyaraj", "_source_table": "users"},
	}

	err := validator.validateSampleDataIntegrity(sourceData, targetData)
	if err != nil {
		t.Errorf("Expected no error for matching data, got %v", err)
	}

	//test with mismatched data
	targetDataMismatch := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "_source_table": "users"},
		{"id": 3, "name": "Sathyaraj", "_source_table": "users"},
	}

	err = validator.validateSampleDataIntegrity(sourceData, targetDataMismatch)
	if err == nil {
		t.Errorf("Expected error for mismatched data, got nil")
	}

}
