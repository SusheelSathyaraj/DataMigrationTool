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

	//Step2: Execute Migration depending on mode
	var migrationErr error
	switch me.Config.Mode {
	case FullMigration:
		migrationErr = me.executeFullMigration(result)
	case IncrementalMigration:
		migrationErr = me.executeIncrementalMigration(result)
	case ScheduledMigration:
		migrationErr = me.executeScheduledMigration(result)
	default:
		return result, fmt.Errorf("unsupported migration mode %s", me.Config.Mode)
	}

	if migrationErr != nil {
		result.Errors = append(result.Errors, migrationErr.Error())
		return result, migrationErr
	}

	//Step3: Post-Migration Validation
	if me.Config.ValidateData {
		postValidation, err := me.Validator.PostMigationValidation(me.Config.Tables, result.PreValidation)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("post migration validation error , v", err))
			return result, fmt.Errorf("Post Migration Validation Failed, %v", err)
		}
		result.PostValidation = postValidation

		postValidationSummary := validation.GenerateValidationSummary(postValidation, startTime)
		postValidationSummary.Print("Post-Migration")

		//check if migration was successful
		if postValidationSummary.InvalidTables > 0 {
			result.Success = false
			return result, fmt.Errorf("migration validation failed for %d tables", postValidationSummary.InvalidTables)
		}
	}

	//Step4: Finalize result
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	result.Success = true
	result.TotalTablesProcessed = len(me.Config.Tables)

	log.Printf("Migration completed successfully in %v ", result.Duration)
	return result, nil
}
