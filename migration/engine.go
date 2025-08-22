package migration

import (
	"fmt"
	"log"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/database"
	"github.com/SusheelSathyaraj/DataMigrationTool/validation"
)

// type of migration
type MigrationMode string

const (
	FullMigration        MigrationMode = "full"
	IncrementalMigration MigrationMode = "incremental"
	ScheduledMigration   MigrationMode = "scheduled"
)

// config for migration
type MigrationConfig struct {
	Mode              MigrationMode
	SourceDb          string
	TargetDb          string
	Tables            []string
	Workers           int
	BatchSize         int
	Concurrent        bool
	ValidateData      bool
	CreateBackup      bool
	IncrementalColumn string //column used for incremental migration like updated_at
}

// Migration process keeper
type MigrationEngine struct {
	Config       MigrationConfig
	SourceClient database.DatabaseClient
	TargetClient database.DatabaseClient
	Validator    *validation.MigrationVaildator
}

// Results of the migration
type MigrationResult struct {
	Success              bool
	TotalTablesProcessed int
	TotalRowsMigrated    int64
	Duration             time.Duration
	PreValidation        []validation.ValidationResult
	PostValidation       []validation.ValidationResult
	Errors               []string
	StartTime            time.Time
	EndTime              time.Time
}

// creating a new migration engine
func NewMigrationEngine(config MigrationConfig, source, target database.DatabaseClient) *MigrationEngine {
	return &MigrationEngine{
		Config:       config,
		SourceClient: source,
		TargetClient: target,
		Validator:    validation.NewMigrationValidator(source, target),
	}
}

// running the complete migration logic
func (me *MigrationEngine) ExecuteMigration() (*MigrationResult, error) {
	startTime := time.Now()

	result := &MigrationResult{
		StartTime: startTime,
		Errors:    make([]string, 0),
	}

	log.Printf("Starting %s migation from %s to %s", me.Config.Mode, me.Config.SourceDb, me.Config.TargetDb)

	//Step1: Premigration  validation
	if me.Config.ValidateData {
		preValidation, err := me.Validator.PreMigrationValidation(me.Config.Tables)
		if err != nil {
			return result, fmt.Errorf("pre-migration validation failed, %v", err)
		}
		result.PreValidation = preValidation

		preValidationSummary := validation.GenerateValidationSummary(preValidation, startTime)
		preValidationSummary.Print("Pre-Migration")

		//checking for any failed tables validation
		if preValidationSummary.InvalidTables > 0 {
			return result, fmt.Errorf("pre-migration validation failed for %d tables", preValidationSummary.InvalidTables)
		}

	}
}
