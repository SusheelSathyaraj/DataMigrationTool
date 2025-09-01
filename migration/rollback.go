package migration

import (
	"log"
	"os"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/database"
	"github.com/SusheelSathyaraj/DataMigrationTool/monitoring"
)

// type to represent a snapshot of the migration state for rollback
type MigrationSnapshot struct {
	ID                string                              `json:"id"`
	Timestamp         time.Time                           `json:"timestamp"`
	SourceDB          string                              `json:"source_db"`
	TargetDB          string                              `json:"target_db"`
	Tables            []string                            `json:"tables"`
	PreMigrationState map[string][]map[string]interface{} `json:"pre_migration_state"`
	MigratedData      map[string][]map[string]interface{} `json:"migrated_data"`
	Status            string                              `json:"status"` //"in_progress", "completed", "failed", "rolled_back"
}

// type to represent a snapshot of the state of the table befoer migration
type TableSnapshot struct {
	TableName     string `json:"table_name"`
	RowCount      int64  `json:"row_count"`
	ExistedBefore bool   `json:"existed_before"`
	SchemaHash    string `json:"schema_hash,omitempty"` //for schema tracking
}

// type for handling migration rollbacks
type RollBackManager struct {
	targetClient database.DatabaseClient
	snapshotsDir string
	logger       *monitoring.MigrationLogger
}

// creating a new rollback manager
func NewRollBackManager(targetClient database.DatabaseClient, logger *monitoring.MigrationLogger) *RollBackManager {
	snapshotsDir := "migration_snapshots"

	//creating snapshots directory if not present
	if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
		log.Printf("Warning: Could not create snaphot directory, %v", err)
	}

	return &RollBackManager{
		targetClient: targetClient,
		snapshotsDir: snapshotsDir,
		logger:       logger,
	}
}
