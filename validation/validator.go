package validation

import (
	"fmt"
	"log"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/database"
)

// Represents the result of the validation check
type ValidationResult struct {
	TableName    string
	IsValid      bool
	ErrorMessage string
	RowCount     int64
	SampleData   []map[string]interface{}
	TimeStamp    time.Time
}

// Handles pre and post migration validation
type MigrationVaildator struct {
	SourceClient database.DatabaseClient
	TargetClient database.DatabaseClient
	SampleSize   int //no. of rows to sample for validation
}

// Creating a new validator instance
func NewMigrationValidator(source, target database.DatabaseClient) *MigrationVaildator {
	return &MigrationVaildator{
		SourceClient: source,
		TargetClient: target,
		SampleSize:   100, //default samplesize
	}
}

// performing validation checks before migration
func (m *MigrationVaildator) PreMigrationValidation(tables []string) ([]ValidationResult, error) {
	log.Println("Starting Premigration Validation ...")

	var results []ValidationResult

	for _, table := range tables {
		result := ValidationResult{
			TableName: table,
			TimeStamp: time.Now(),
		}

		//checking if table is present and getting the row count
		sourceData, err := m.SourceClient.FetchAllData([]string{table})
		if err != nil {
			result.IsValid = false
			result.ErrorMessage = fmt.Sprintf("Failed to fetch data from the source table %s:%v", table, err)
			results = append(results, result)
			continue
		}

		result.RowCount = int64(len(sourceData))
		result.IsValid = true

		//getting samples for validation
		sampleSize := m.SampleSize
		if len(sourceData) < sampleSize {
			sampleSize = len(sourceData)
		}
		result.SampleData = sourceData[:sampleSize]

		log.Printf("Pre-Validation: Table %s contains %d rows", table, result.RowCount)
		results = append(results, result)
	}
	return results, nil
}

// performing validation checks after migration completion
func (m *MigrationVaildator) PostMigationValidation(tables []string, preValidationResults []ValidationResult) ([]ValidationResult, error) {
	log.Println("Starting Post Migration Validation...")

	var results []ValidationResult

	//creating a map for quick lookupof pre-validation results
	preResultMap := make(map[string]ValidationResult)
	for _, result := range preValidationResults {
		preResultMap[result.TableName] = result
	}

	for _, table := range tables {
		result := ValidationResult{
			TableName: table,
			TimeStamp: time.Now(),
		}

		//getting target data
		targetData, err := m.TargetClient.FetchAllData([]string{table})
		if err != nil {
			result.IsValid = false
			result.ErrorMessage = fmt.Sprintf("Failed to fetch data from target table %s, %v", table, err)
			results = append(results, result)
			continue
		}

		result.RowCount = int64(len(targetData))

		//comparing with source data count
		preResult, exists := preResultMap[table]
		if !exists {
			result.IsValid = false
			result.ErrorMessage = fmt.Sprintf("No prevalidation data found for table %s", table)
			results = append(results, result)
			continue
		}

		if result.RowCount != preResult.RowCount {
			result.IsValid = false
			result.ErrorMessage = fmt.Sprintf("Row count mismatch, expected source: %d, got target: %d", preResult.RowCount, result.RowCount)
			results = append(results, result)
			continue
		}

		//sample data validation
		if len(targetData) > 0 {
			sampleSize := m.SampleSize
			if len(targetData) < sampleSize {
				sampleSize = len(targetData)
			}
			result.SampleData = targetData[:sampleSize]

			//Validating sample data integrity
			if err := m.validateSampleDataIntegrity(preResult.SampleData, result.SampleData); err != nil {
				result.IsValid = false
				result.ErrorMessage = fmt.Sprintf("Data integrity Validation failed, %v ", err)
				results = append(results, result)
				continue
			}
		}
		result.IsValid = true
		log.Printf("Post-validation: Table %s successfully migrated with %d rows", table, result.RowCount)
		results = append(results, result)
	}
	return results, nil
}

// comparing sample data from source and target
func (m *MigrationVaildator) validateSampleDataIntegrity(sourceData, targetData []map[string]interface{}) error {
	if len(sourceData) == 0 && len(targetData) == 0 {
		return nil
	}

	if len(sourceData) != len(targetData) {
		return fmt.Errorf("sample data length mismatch, source:%d, target:%d", len(sourceData), len(targetData))
	}

	//check first few rows for data integrity
	checkRows := 5
	if len(sourceData) < checkRows {
		checkRows = len(sourceData)
	}

	for i := 0; i < checkRows; i++ {
		sourceRow := sourceData[i]
		targetRow := targetData[i]

		//removing metadata fields for comparison
		cleanSourceRow := make(map[string]interface{})
		cleanTargetRow := make(map[string]interface{})

		for k, v := range sourceRow {
			if k != "_source_table" {
				cleanSourceRow[k] = v
			}
		}
		for k, v := range targetRow {
			if k != "_source_table" {
				cleanTargetRow[k] = v
			}
		}

		//comparing key fields (assuming first non-metadata field is primary key)
		var primaryKey string
		for k := range cleanSourceRow {
			primaryKey = k
			break
		}
		if primaryKey != "" {
			sourceVal := cleanSourceRow[primaryKey]
			targetVal := cleanTargetRow[primaryKey]

			if !compareValues(sourceVal, targetVal) {
				return fmt.Errorf("primary key mismatch in row %d: source: %v, target:%v", i, sourceVal, targetVal)
			}
		}
	}
	return nil
}

// comparing two values handling type conversion
func compareValues(v1, v2 interface{}) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	//handling string conversions
	str1 := fmt.Sprintf("%v", v1)
	str2 := fmt.Sprintf("%v", v2)

	return str1 == str2
}

// struct for validation result summary
type ValidationSummary struct {
	TotalTables    int
	ValidTables    int
	InvalidTables  int
	TotalRows      int64
	ValidationTime time.Duration
	Errors         []string
}

// creating a summary of the validation result
func GenerateValidationSummary(results []ValidationResult, startTime time.Time) ValidationSummary {
	summary := ValidationSummary{
		TotalTables:    len(results),
		ValidationTime: time.Since(startTime),
		Errors:         make([]string, 0),
	}

	for _, result := range results {
		summary.TotalRows += result.RowCount

		if result.IsValid {
			summary.ValidTables++
		} else {
			summary.InvalidTables++
			summary.Errors = append(summary.Errors, fmt.Sprintf("Table %s:%s", result.TableName, result.ErrorMessage))
		}
	}
	return summary
}

// printing the formatted summary
func (s ValidationSummary) Print(phase string) {
	fmt.Printf("\n==%s Validation Summary==\n", phase)
	fmt.Printf("Total Tables: %d\n", s.TotalTables)
	fmt.Printf("Valid Tables: %d\n", s.ValidTables)
	fmt.Printf("Invalid Tables: %d\n", s.InvalidTables)
	fmt.Printf("Total Rows: %d\n", s.TotalRows)
	fmt.Printf("Validation Time: %v\n", s.ValidationTime)

	if len(s.Errors) > 0 {
		fmt.Println("Errors:")
		for _, err := range s.Errors {
			fmt.Printf("-%s\n", err)
		}
	}
	fmt.Println("--------------")
}
