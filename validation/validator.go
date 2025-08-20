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
