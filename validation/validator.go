package validation

import (
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
