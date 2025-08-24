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
			result.Errors = append(result.Errors, fmt.Sprintf("post migration validation error , %v", err))
			return result, fmt.Errorf("post Migration Validation Failed, %v", err)
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

// performs a complete full data migration
func (me *MigrationEngine) executeFullMigration(result *MigrationResult) error {
	log.Printf("Executing Full Migration...")

	//fetching all data from source
	var sourceData []map[string]interface{}
	var err error

	if me.Config.Concurrent && len(me.Config.Tables) > 1 {
		log.Printf("Using concurrent processing with %d workers", me.Config.Workers)
		sourceData, err = me.SourceClient.FetchAllDataConcurrently(me.Config.Tables, me.Config.Workers)
	} else {
		log.Println("Using sequential processing")
		sourceData, err = me.SourceClient.FetchAllData(me.Config.Tables)
	}

	if err != nil {
		return fmt.Errorf("failed to fetch source data, %v", err)
	}

	result.TotalRowsMigrated = int64(len(sourceData))
	log.Printf("Fetched %d rows from source database", result.TotalRowsMigrated)

	//validating data types before migration
	if me.Config.ValidateData {
		if err := me.Validator.ValidateDataTypes(sourceData); err != nil {
			return fmt.Errorf("data type validation failed, %v", err)
		}
	}

	//importing data to target database
	if me.Config.Concurrent && len(sourceData) > me.Config.BatchSize {
		log.Printf("Using concurrent batch processing with batch size %d", me.Config.BatchSize)
		err = me.SourceClient.ImportDataConcurrently(sourceData, me.Config.BatchSize)
	} else {
		log.Println("Using sequential import")
		err = me.SourceClient.ImportData(sourceData)
	}
	if err != nil {
		return fmt.Errorf("failed to import data to target, %v", err)
	}

	log.Printf("Successfully migrated %d rows to target database", result.TotalRowsMigrated)
	return nil
}

// performing incremental data migration(placeholder)
func (me *MigrationEngine) executeIncrementalMigration(result *MigrationResult) error {
	log.Println("Executing 	incremental migration...")
	//TODO: implement incremental migration logic
	//1.identify changed records since last migration
	//2.fetching only the delta data
	//3.performing upsert operations on target
	return fmt.Errorf("incremental migration not implemented")
}

// performing scheduled data migration(placeholder)
func (me *MigrationEngine) executeScheduledMigration(result *MigrationResult) error {
	log.Println("Executing Scheduled Migration...")
	//TODO: implent scheduled migration logic
	//1.setting up cron jobs
	//2.managing job state
	//3.handling concurrent job execution
	return fmt.Errorf("scheduled migration not implemented")
}

// printing the formatted result of migration
func (mr *MigrationResult) Print() {
	fmt.Println("\n=== Migration Result===")
	fmt.Printf("Success: %v\n", mr.Success)
	fmt.Printf("Duration %v\n", mr.Duration)
	fmt.Printf("Tables Processed %v\n", mr.TotalTablesProcessed)
	fmt.Printf("Rows Migrated %v\n", mr.TotalRowsMigrated)
	fmt.Printf("Start Time %s\n", mr.StartTime.Format("2025-08-24 20:09:45"))
	fmt.Printf("End Time %s\n", mr.EndTime.Format("2025-08-24 20:09:45"))

	if len(mr.Errors) > 0 {
		fmt.Println("\n Errors:")
		for _, err := range mr.Errors {
			fmt.Printf("-%s\n", err)
		}
	}
	fmt.Println("===============")
}

// Rollback for a failed migration (placeholder)
func (me *MigrationEngine) RollbackMigration() error {
	log.Println("Attempting migration rollback...")

	//TODO: implement rollback logic
	//1.identify what was migrated
	//2.removing migrated data from target
	//restore from backup if available

	return fmt.Errorf("rollback functionality not implemented")
}
