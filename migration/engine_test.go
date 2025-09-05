package migration

import (
	"fmt"
	"testing"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/test"
)

func TestMigrationEngineFullMigration(t *testing.T) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")
	targetClient := test.NewCompleteMockDatabaseClient("postgresql")

	//Adding test data
	testData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "age": 27, "email": "susheel@example.com"},
		{"id": 2, "name": "Sathyaraj", "age": 28, "email": "sathyaraj@example.com"},
		{"id": 3, "name": "SusheelSathyaraj", "age": 29, "email": "susheelsathyaraj@example.com"},
	}
	sourceClient.AddTestData("users", testData)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		Workers:      2,
		BatchSize:    1000,
		Concurrent:   false,
		ValidateData: true,
		CreateBackup: true,
	}

	//connecting clients
	if err := sourceClient.Connect(); err != nil {
		t.Fatalf("Failed to connect source client, %v", err)
	}
	defer sourceClient.Close()

	if err := targetClient.Connect(); err != nil {
		t.Fatalf("Failed to connected to the target client, %v", err)
	}
	defer targetClient.Close()

	engine := NewMigrationEngine(config, sourceClient, targetClient)

	//execute migration
	result, err := engine.ExecuteMigration()

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	if !result.Success {
		t.Errorf("Expected successful migration, got failure")
		if len(result.Errors) > 0 {
			t.Errorf("Migration Errors, %v", result.Errors)
		}
	}

	if result.TotalRowsMigrated != 3 {
		t.Errorf("Expected 2 rows to be migrated, got %d", result.TotalRowsMigrated)
	}

	if result.TotalTablesProcessed != 1 {
		t.Errorf("Expected 1 table to be processed, got %d", result.TotalTablesProcessed)
	}

	//checking if data if imported
	importedData := targetClient.GetImportedData("users")
	if len(importedData) != 3 {
		t.Errorf("Expect 3 rows to be imported, got %d", len(importedData))
	}

	if importedData[0]["name"] != "Susheel" {
		t.Errorf("Expected the first user to be Susheel, got %s instead", importedData[0]["name"])
	}

	//verifying call counts
	if sourceClient.GetFetchCallCount() == 0 {
		t.Errorf("Expected fetch to be called on sourceclient")
	}

	if targetClient.GetImportCallCount() == 0 {
		t.Errorf("Expected import to be called on targetclient ")
	}
}

func TestMigrationEngineWithConnectionFailure(t *testing.T) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")

	//making the source client fail on connect
	sourceClient.SetFailOnConnect(true)

	//trying to connect- should fail
	err := sourceClient.Connect()
	if err != nil {
		t.Errorf("Expected the connection to fail, but it succeeded")
	}
	//verifying connection state
	if sourceClient.IsConnected() {
		t.Errorf("Expected the connection to not be established")
	}
}

func TestMigrationEngineWithFetchError(t *testing.T) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")
	targerClient := test.NewCompleteMockDatabaseClient("postgresql")

	//setting source to fail on fetch
	sourceClient.SetFailOnFetch("users")

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgesql",
		Tables:       []string{"users"},
		ValidateData: false,
		CreateBackup: false,
	}

	//connecting clients
	sourceClient.Connect()
	targerClient.Connect()
	defer sourceClient.Close()
	defer targerClient.Close()

	engine := NewMigrationEngine(config, sourceClient, targerClient)

	result, err := engine.ExecuteMigration()

	if err == nil {
		t.Errorf("Expected error due to fetch failure, got nil")
	}

	if result == nil {
		t.Fatal("Expected result even on failure, got nil")
	}

	if result.Success {
		t.Errorf("Expected migration failure, got success")
	}

	if len(result.Errors) == 0 {
		t.Errorf("Expected errors in result, got none")
	}
}

func TestMigrationEngineWithImportError(t *testing.T) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")
	targetClient := test.NewCompleteMockDatabaseClient("postgresql")

	testData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "age": 30},
	}
	sourceClient.AddTestData("users", testData)

	targetClient.SetFailOnImport(true)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		ValidateData: false, //disabling validation as it is for testing import error
		CreateBackup: false, //disabling for simpler test
	}

	sourceClient.Connect()
	targetClient.Connect()
	defer sourceClient.Close()
	defer targetClient.Close()

	engine := NewMigrationEngine(config, sourceClient, targetClient)

	result, err := engine.ExecuteMigration()

	if err == nil {
		t.Errorf("Expected error due to import failure, got nil")
	}

	if result != nil && result.Success {
		t.Errorf("Expected migration failure, got success")
	}

	//verifying no data was imported
	if targetClient.GetTotalImportedRows() > 0 {
		t.Errorf("Expected no imported data due to failure, got %d rows", targetClient.GetTotalImportedRows())
	}
}

func TestMigrationEngineMultipleTables(t *testing.T) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")
	targetClient := test.NewCompleteMockDatabaseClient("postgresql")

	//add data for multiple tables
	usersData := []map[string]interface{}{
		{"id": 1, "name": "Susheel"},
		{"id": 2, "name": "Sathyaraj"},
	}

	ordersData := []map[string]interface{}{
		{"id": 1, "user_id": 1, "amount": 100.50},
		{"id": 2, "user_id": 2, "amount": 10},
		{"id": 3, "user_id": 1, "amount": 5070.50},
	}

	sourceClient.AddTestData("users", usersData)
	sourceClient.AddTestData("orders", ordersData)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users", "orders"},
		ValidateData: true,
	}

	sourceClient.Connect()
	targetClient.Connect()
	defer sourceClient.Close()
	defer targetClient.Close()

	engine := NewMigrationEngine(config, sourceClient, targetClient)

	result, err := engine.ExecuteMigration()

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful migration, got failure")
		if len(result.Errors) > 0 {
			t.Errorf("Migration errors, %v", result.Errors)
		}
	}

	if result.TotalRowsMigrated != 5 {
		t.Errorf("Expected 5 rows to be migrated, got %d", result.TotalRowsMigrated)
	}

	if result.TotalTablesProcessed != 2 {
		t.Errorf("Expected 2 tables to be processed, got %d", result.TotalTablesProcessed)
	}

	//checking for import data
	importedUsers := targetClient.GetImportedData("users")
	importedOrders := targetClient.GetImportedData("orders")

	if len(importedUsers) != len(usersData) {
		t.Errorf("Expected %d imported users, found %d", len(importedUsers), len(usersData))
	}

	if len(importedOrders) != len(ordersData) {
		t.Errorf("Expected %d imported orders, found %d", len(importedOrders), len(ordersData))
	}

	if result.Duration == 0 {
		t.Errorf("Expected Migration duration >0, got %v", result.Duration)
	}

	//log performance metrics for manual review
	avgSpeed := float64(result.TotalRowsMigrated) / result.Duration.Seconds()
	t.Logf("Migration Performance:")
	t.Logf(" Total Time,  %v", result.Duration)
	t.Logf(" Rows/Second, %.2f", avgSpeed)
	t.Logf(" Tables %d", result.TotalTablesProcessed)
}

func TestMigrationEngineWithConcurrentProcessing(t *testing.T) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")
	targetClient := test.NewCompleteMockDatabaseClient("postgresql")

	testData := []map[string]interface{}{
		{"id": 1, "name": "Susheel"},
		{"id": 2, "name": "Sathyaraj"},
	}
	sourceClient.AddTestData("users", testData)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		BatchSize:    1,
		Workers:      2,
		Concurrent:   true,
		ValidateData: true,
	}

	//add data for multiple tables
	usersData := []map[string]interface{}{
		{"id": 1, "name": "Susheel"},
		{"id": 2, "name": "Sathyaraj"},
	}

	ordersData := []map[string]interface{}{
		{"id": 1, "user_id": 1, "amount": 100.50},
		{"id": 2, "user_id": 2, "amount": 10},
		{"id": 3, "user_id": 1, "amount": 5070.50},
	}

	sourceClient.AddTestData("users", usersData)
	sourceClient.AddTestData("orders", ordersData)

	sourceClient.Connect()
	targetClient.Connect()
	defer sourceClient.Close()
	defer targetClient.Close()

	engine := NewMigrationEngine(config, sourceClient, targetClient)

	result, err := engine.ExecuteMigration()

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful migration, got failure")
		if len(result.Errors) > 0 {
			t.Errorf("Migraiton Errors: %v", result.Errors)
		}
	}

	expectedTotalRows := int64(len(usersData) + len(ordersData))
	if result.TotalRowsMigrated != expectedTotalRows {
		t.Errorf("Expected %d rows to be migrated, got %d", expectedTotalRows, result.TotalRowsMigrated)
	}

	if result.TotalTablesProcessed != 2 {
		t.Errorf("Expected 2 tables processed, got %d", result.TotalTablesProcessed)
	}

	importedUsers := targetClient.GetImportedData("users")
	importedOrders := targetClient.GetImportedData("orders")

	if len(importedUsers) != len(usersData) {
		t.Errorf("Expected %d users to be imported, got %d", len(importedUsers), len(usersData))
	}

	if len(importedOrders) != len(ordersData) {
		t.Errorf("Expected %d orders to be imported, got %d", len(importedOrders), len(ordersData))
	}

	if result.Duration == 0 {
		t.Errorf("Expected Migration duration >0, got %v", result.Duration)
	}

	//log performance metrics for manual review
	avgSpeed := float64(result.TotalRowsMigrated) / result.Duration.Seconds()
	t.Logf("Migration Performance:")
	t.Logf(" Total Time,  %v", result.Duration)
	t.Logf(" Rows/Second, %.2f", avgSpeed)
	t.Logf(" Tables %d", result.TotalTablesProcessed)
}

func TestMigrationEngineWithBackupAndRollBack(t *testing.T) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")
	targetClient := test.NewCompleteMockDatabaseClient("postgresql")

	testData := []map[string]interface{}{
		{"id": 1, "name": "Susheel", "status": "active"},
	}

	sourceClient.AddTestData("users", testData)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		ValidateData: false,
		CreateBackup: true,
	}

	sourceClient.Connect()
	targetClient.Connect()
	defer sourceClient.Close()
	defer targetClient.Close()

	engine := NewMigrationEngine(config, sourceClient, targetClient)
	result, err := engine.ExecuteMigration()

	if err != nil {
		t.Fatalf("Migration Failed, %v", err)
	}

	if !result.Success {
		t.Errorf("Expected successful migration, got failure")
	}

	//verifying that backup is created
	if engine.CurrentSnapshot == nil {
		t.Errorf("Expected backup snapshot to be cretaed")
	} else {
		t.Logf("Backup snapshot created, %s", engine.CurrentSnapshot.ID)
	}

	//verfying rollback functionality
	rollbackErr := engine.RollBackManager.RollBackMigration(engine.CurrentSnapshot.ID)
	if rollbackErr != nil {
		t.Logf("Rollback failed(expected for mock implementation), %v", rollbackErr)
		//this is for now expected since complete rollbak is not iplemented yet
	}
}

func TestMigrationEngineIncrementalMode(t *testing.T) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")
	targetCleint := test.NewCompleteMockDatabaseClient("postgresql")

	config := MigrationConfig{
		Mode:         IncrementalMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		ValidateData: false,
		CreateBackup: false,
	}

	sourceClient.Connect()
	targetCleint.Connect()
	defer sourceClient.Close()
	defer targetCleint.Close()

	engine := NewMigrationEngine(config, sourceClient, targetCleint)
	result, err := engine.ExecuteMigration()

	if err == nil {
		t.Errorf("Expected error as incremental migration is not implemented, got nil")
	}

	if result != nil && result.Success {
		t.Errorf("Expected migration failure due to unimplemented feature, got success")
	}
}

func TestMigrationEngineScheduledMode(t *testing.T) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")
	targetClient := test.NewCompleteMockDatabaseClient("postgresql")

	config := MigrationConfig{
		Mode:         ScheduledMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		ValidateData: false,
		CreateBackup: false,
	}

	sourceClient.Connect()
	targetClient.Connect()
	defer sourceClient.Close()
	defer targetClient.Close()

	engine := NewMigrationEngine(config, sourceClient, targetClient)
	result, err := engine.ExecuteMigration()

	if err == nil {
		t.Errorf("Expected error as scheduled migration is not implemented, got nil")
	}

	if result != nil && result.Success {
		t.Errorf("Expected migration failure due to unimplemented feature, got success")
	}
}

func TestMigrationResultPrint(t *testing.T) {
	result := &MigrationResult{
		Success:              true,
		TotalTablesProcessed: 2,
		TotalRowsMigrated:    150,
		Duration:             5 * time.Minute,
		StartTime:            time.Now().Add(-5 * time.Minute),
		EndTime:              time.Now(),
		Errors:               []string{"Warning:Large table detected"},
	}

	//checks to ensure that print does not panic
	result.Print()
}

func TestMigrationConfigValidation(t *testing.T) {
	testCases := []struct {
		config      MigrationConfig
		expectError bool
		description string
	}{
		{
			config: MigrationConfig{
				Mode:     FullMigration,
				SourceDb: "mysql",
				TargetDb: "postgesql",
				Tables:   []string{"users"},
			},
			expectError: false,
			description: "Valid Full Migration config",
		},
		{
			config: MigrationConfig{
				Mode:     "invalid",
				SourceDb: "mysql",
				TargetDb: "postgresql",
				Tables:   []string{"users"},
			},
			expectError: true,
			description: "Invalid Migration Mode",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			sourceClient := test.NewCompleteMockDatabaseClient("mysql")
			targetCLient := test.NewCompleteMockDatabaseClient("postgresql")

			if tc.config.Mode == FullMigration {
				//testdata for valid cases
				testData := []map[string]interface{}{
					{"id": 1, "name": "Susheel"},
				}
				sourceClient.AddTestData("users", testData)
			}
			engine := NewMigrationEngine(tc.config, sourceClient, targetCLient)
			_, err := engine.ExecuteMigration()

			if tc.expectError && err == nil {
				t.Errorf("Expected error for %s, got nil", tc.description)
			} else if !tc.expectError && err != nil {
				t.Errorf("Expected no error for %s, got %v", tc.description, err)
			}
		})
	}
}

func BenchmarkMigrationEngineFull(b *testing.B) {
	sourceClient := test.NewCompleteMockDatabaseClient("mysql")

	//adding large test dataset
	var testData []map[string]interface{}
	for i := 0; i < 1000; i++ {
		testData = append(testData, map[string]interface{}{
			"id":     i,
			"name":   fmt.Sprintf("User%d", i),
			"email":  fmt.Sprintf("user%d@example.com", i),
			"age":    25 + (i % 50),
			"status": "active",
			"score":  float64(i * 10),
			"active": i%2 == 0,
		})
	}

	sourceClient.AddTestData("users", testData)

	config := MigrationConfig{
		Mode:         FullMigration,
		SourceDb:     "mysql",
		TargetDb:     "postgresql",
		Tables:       []string{"users"},
		Workers:      4,
		BatchSize:    100,
		Concurrent:   true,
		ValidateData: false, //disabling as it is benchmark
		CreateBackup: false, //disabing as it is benchmark
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//resetting target client for each iteration
		targetClient := test.NewCompleteMockDatabaseClient("postgresql")

		sourceClient.Connect()
		targetClient.Connect()

		engine := NewMigrationEngine(config, sourceClient, targetClient)
		_, err := engine.ExecuteMigration()

		sourceClient.Close()
		targetClient.Close()

		if err != nil {
			b.Fatal(err)
		}
	}
}
