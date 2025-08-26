package migration

import (
	"fmt"
	"log"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/database"
	"github.com/SusheelSathyaraj/DataMigrationTool/monitoring"
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
	Config          MigrationConfig
	SourceClient    database.DatabaseClient
	TargetClient    database.DatabaseClient
	Validator       *validation.MigrationVaildator
	ProgressTracker *monitoring.ProcessTracker
	Logger          *monitoring.MigrationLogger
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
	//initialising with estimated row count(will be updated during validation)
	progressTracker := monitoring.NewProgressTracker(0, len(config.Tables))
	logger := monitoring.NewMigrationLogger()

	return &MigrationEngine{
		Config:          config,
		SourceClient:    source,
		TargetClient:    target,
		Validator:       validation.NewMigrationValidator(source, target),
		ProgressTracker: progressTracker,
		Logger:          logger,
	}
}

// running the complete migration logic
func (me *MigrationEngine) ExecuteMigration() (*MigrationResult, error) {
	startTime := time.Now()

	result := &MigrationResult{
		StartTime: startTime,
		Errors:    make([]string, 0),
	}

	me.Logger.Info(fmt.Sprintf("Starting %s migration from %s to %s", me.Config.Mode, me.Config.SourceDb, me.Config.TargetDb))
	log.Printf("Starting %s migation from %s to %s", me.Config.Mode, me.Config.SourceDb, me.Config.TargetDb)

	//Step1: Premigration  validation
	if me.Config.ValidateData {
		me.Logger.Info("Starting Pre-Migration Validation")
		preValidation, err := me.Validator.PreMigrationValidation(me.Config.Tables)
		if err != nil {
			me.Logger.Error("Pre-Migration Validation failed", err.Error())
			return result, fmt.Errorf("pre-migration validation failed, %v", err)
		}
		result.PreValidation = preValidation

		//updating progress tracker with actual row count
		var totalRows int64
		for _, validation := range preValidation {
			totalRows += validation.RowCount
		}
		me.ProgressTracker = monitoring.NewProgressTracker(totalRows, len(me.Config.Tables))

		preValidationSummary := validation.GenerateValidationSummary(preValidation, startTime)
		preValidationSummary.Print("Pre-Migration")

		//checking for any failed tables validation
		if preValidationSummary.InvalidTables > 0 {
			me.Logger.Error("Pre-Migration Validation failed", fmt.Sprintf("%d tables failed validation", preValidationSummary.InvalidTables))
			return result, fmt.Errorf("pre-migration validation failed for %d tables", preValidationSummary.InvalidTables)
		}
		me.Logger.Info(fmt.Sprintf("Pre-Migration Validation Completed Successfully - %d rows across %d tables", totalRows, len(me.Config.Tables)))
	}

	//Starting progress monitoring
	stopProgress := me.ProgressTracker.StartProgressMonitor(2 * time.Second)
	defer func() {
		stopProgress <- struct{}{}
		me.ProgressTracker.PrintFinalSummary()
		me.Logger.Close()
	}()

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
		me.Logger.Error("Migration Failed", migrationErr.Error())
		result.Errors = append(result.Errors, migrationErr.Error())
		return result, migrationErr
	}

	//Step3: Post-Migration Validation
	if me.Config.ValidateData {
		me.Logger.Info("Starting Post-Migration Validation")
		postValidation, err := me.Validator.PostMigationValidation(me.Config.Tables, result.PreValidation)
		if err != nil {
			me.Logger.Error("Post-Migration VAlidation error", err.Error())
			result.Errors = append(result.Errors, fmt.Sprintf("post migration validation error , %v", err))
			return result, fmt.Errorf("post Migration Validation Failed, %v", err)
		}
		result.PostValidation = postValidation

		postValidationSummary := validation.GenerateValidationSummary(postValidation, startTime)
		postValidationSummary.Print("Post-Migration")

		//check if migration was successful
		if postValidationSummary.InvalidTables > 0 {
			me.Logger.Error("Migration Validation failed", fmt.Sprintf("%d tables failed post migration validation", postValidationSummary.InvalidTables))
			result.Success = false
			return result, fmt.Errorf("migration validation failed for %d tables", postValidationSummary.InvalidTables)
		}
		me.Logger.Info("Post-Migration VAlidation completed Successfully")
	}

	//Step4: Finalize result
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	result.Success = true
	result.TotalTablesProcessed = len(me.Config.Tables)

	me.Logger.Info(fmt.Sprintf("Migration completed Successfully in %v", result.Duration))
	log.Printf("Migration completed successfully in %v ", result.Duration)
	return result, nil
}

// performs a complete full data migration
func (me *MigrationEngine) executeFullMigration(result *MigrationResult) error {
	me.Logger.Info("Executing Full Migration")
	log.Printf("Executing Full Migration...")

	//processing tables individually for better tracking
	for i, table := range me.Config.Tables {
		me.ProgressTracker.SetCurrentTable(table)
		me.Logger.TableProgress(table, 0, "Starting table Migration")

		//fetching data from current table
		var tableData []map[string]interface{}
		var err error

		if me.Config.Concurrent && len(me.Config.Tables) > 1 {
			tableData, err = me.SourceClient.FetchAllDataConcurrently([]string{table}, 1)
		} else {
			tableData, err = me.SourceClient.FetchAllData([]string{table})
		}

		if err != nil {
			errorMsg := fmt.Sprintf("failed to fetch data from table %s, %v", table, err)
			me.Logger.Error("Table Fetching Failed", errorMsg)
			me.ProgressTracker.AddError(errorMsg)
			return fmt.Errorf(errorMsg)
		}

		tableRowCount := int64(len(tableData))
		me.Logger.TableProgress(table, tableRowCount, fmt.Sprintf("Fetched %d rows ", tableRowCount))

		//validating data types before migration
		if me.Config.ValidateData && len(tableData) > 0 {
			if err := me.Validator.ValidateDataTypes(tableData); err != nil {
				errorMsg := fmt.Sprintf("data type validation failed for table %s, %v", table, err)
				me.Logger.Error("Data Type Validation Failed", errorMsg)
				me.ProgressTracker.AddError(errorMsg)
				return fmt.Errorf(errorMsg)
			}
		}

		//importing data to target database with batch tracking
		if me.Config.Concurrent && len(tableData) > me.Config.BatchSize {
			me.Logger.TableProgress(table, tableRowCount, fmt.Sprintf("Starting Concurrent import with batchsize %d", me.Config.BatchSize))

			//creating batch tracker for this table
			batchTracker := me.ProgressTracker.NewBatchTracker(me.Config.BatchSize)

			//overriding the import to track batched
			err = me.importDataWithBatchTracking(tableData, batchTracker)
		} else {
			me.Logger.TableProgress(table, tableRowCount, "Starting Sequential Import")
			err = me.TargetClient.ImportData(tableData)
			me.ProgressTracker.UpdateProgress(tableRowCount)
		}

		if err != nil {
			errorMsg := fmt.Sprintf("failed to import data for table %s, %v", table, err)
			me.Logger.Error("Table Import Failed", errorMsg)
			me.ProgressTracker.AddError(errorMsg)
			return fmt.Errorf(errorMsg)
		}

		me.ProgressTracker.CompletedTable()
		me.Logger.TableProgress(table, tableRowCount, "Table Migration Completed Successfully")
		result.TotalRowsMigrated += tableRowCount

		log.Printf("Successfully migrated table %s (%d/%d) with %d rows", table, i+1, len(me.Config.Tables), tableRowCount)
	}

	me.Logger.Info(fmt.Sprintf("Full Migration Completed -%d rows migrated", result.TotalRowsMigrated))
	log.Printf("Successfully Migrated %d rows across %d tables", result.TotalRowsMigrated, len(me.Config.Tables))

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

// importing data with detail batch progress trackking
func (me *MigrationEngine) importDataWithBatchTracking(data []map[string]interface{}, batchTracker *monitoring.BatchTracker) error {
	batchSize := me.Config.BatchSize
	totalBatches := (len(data) + batchSize - 1) / batchSize //ceiling division

	for i := 0; i < len(data); i++ {
		end := i + batchSize
		if end > len(data) {
			end = len(data)
		}
		batch := data[i:end]
		batchNumber := (i / batchSize) + 1

		batchTracker.StartBatch(batchNumber)

		//importing the batch
		if err := me.TargetClient.ImportData(batch); err != nil {
			return fmt.Errorf("failed to import batch %d / %d, %v", batchNumber, totalBatches, err)
		}
		batchTracker.CompleteBatch(int64(len(batch)))
	}
	return nil
}
