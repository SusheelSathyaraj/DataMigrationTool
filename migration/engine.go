package migration

import (
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
