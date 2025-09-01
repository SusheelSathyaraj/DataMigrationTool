package migration

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	PreMigrationState map[string]TableSnapshot            `json:"pre_migration_state"`
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

// creating a snapshot before migation
func (rm *RollBackManager) CreateSnapshot(config MigrationConfig) (*MigrationSnapshot, error) {
	snapshotID := fmt.Sprintf("migration_%s_to_%s_%d", config.SourceDb, config.TargetDb, time.Now().Unix())

	rm.logger.Info(fmt.Sprintf("Creating migration snapshot, %s", snapshotID))

	snapshot := &MigrationSnapshot{
		ID:                snapshotID,
		Timestamp:         time.Now(),
		SourceDB:          config.SourceDb,
		TargetDB:          config.TargetDb,
		Tables:            config.Tables,
		PreMigrationState: make(map[string]TableSnapshot),
		MigratedData:      make(map[string][]map[string]interface{}),
		Status:            "in_progress",
	}

	//capturing pre-migration state for each tble
	for _, table := range config.Tables {
		tableSnapshot, err := rm.captureTableState(table)
		if err != nil {
			rm.logger.Error("Failed to capture table state", fmt.Sprintf("Table: %s, Error: %v", table, err))
			//continue with othe tables  instead of failing completely
			tableSnapshot = TableSnapshot{
				TableName:     table,
				RowCount:      0,
				ExistedBefore: false,
			}
		}
		snapshot.PreMigrationState[table] = tableSnapshot
	}
	//saving snapshot to tthe disk
	if err := rm.saveSnapshot(snapshot); err != nil {
		return nil, fmt.Errorf("failed to save snapshot, %v", err)
	}
	rm.logger.Info(fmt.Sprintf("Migration snapshot created Successfully, %s", snapshotID))
	return snapshot, nil
}

// capturing the current state of the table
func (rm *RollBackManager) captureTableState(tableName string) (TableSnapshot, error) {
	//fetching existing data to check if table exists and get row count
	existingData, err := rm.targetClient.FetchAllData([]string{tableName})

	if err != nil {
		//table might not exist, which is fine for fresh migration
		return TableSnapshot{
			TableName:     tableName,
			RowCount:      0,
			ExistedBefore: false,
		}, nil
	}

	return TableSnapshot{
		TableName:     tableName,
		RowCount:      int64(len(existingData)),
		ExistedBefore: true,
	}, nil
}

// saving a snapshot to the disc
func (rm *RollBackManager) saveSnapshot(snapshot *MigrationSnapshot) error {
	fileName := filepath.Join(rm.snapshotsDir, snapshot.ID+".json")

	data, err := json.MarshalIndent(snapshot, "", " ")
	if err != nil {
		return fmt.Errorf("failed to Marshal snapshot, %v", err)
	}
	if err := os.WriteFile(fileName, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot file, %v", err)
	}
	return nil
}

// updating the snapshot with migrated data for rollback
func (rm *RollBackManager) UpdateSnapshotWithMigratedData(snapshotID string, data []map[string]interface{}) error {
	snapshot, err := rm.LoadSnapshot(snapshotID)
	if err != nil {
		return fmt.Errorf("failed to load snapshot, %v", err)
	}

	//group data by table
	for _, row := range data {
		if tableName, ok := row["_source_table"].(string); ok {
			if snapshot.MigratedData[tableName] == nil {
				snapshot.MigratedData[tableName] = make([]map[string]interface{}, 0)
			}

			//Storing the migrated row to potential rollbac
			cleanRow := make(map[string]interface{})
			for k, v := range row {
				if k != "_source_table" {
					cleanRow[k] = v
				}
			}
			snapshot.MigratedData[tableName] = append(snapshot.MigratedData[tableName], cleanRow)
		}
	}
	return rm.saveSnapshot(snapshot)
}

// loading the snapshot to the disc
func (rm *RollBackManager) LoadSnapshot(snapshotID string) (*MigrationSnapshot, error) {
	filename := filepath.Join(rm.snapshotsDir, snapshotID+".json")

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot file, %v", err)
	}

	var snapshot MigrationSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarhsal snapshot, %v", err)
	}
	return &snapshot, nil
}

// marking the snapshot as completed
func (rm *RollBackManager) MarkSnapshotCompleted(snapshotID string) error {
	snapshot, err := rm.LoadSnapshot(snapshotID)
	if err != nil {
		return err
	}

	snapshot.Status = "completed"
	return rm.saveSnapshot(snapshot)
}

// marking the snapshot as failed
func (rm *RollBackManager) MarkSnapshotFailed(snapshotID string) error {
	snapshot, err := rm.LoadSnapshot(snapshotID)
	if err != nil {
		return err
	}
	snapshot.Status = "failed"
	return rm.saveSnapshot(snapshot)
}
