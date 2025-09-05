package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/config"
	"github.com/SusheelSathyaraj/DataMigrationTool/migration"

	"github.com/SusheelSathyaraj/DataMigrationTool/database"
)

// supported database formats
var supportedDatabases = []string{"mysql", "postgresql", "mongodb"}

//validate inputs, source, target, filetype and mode

func validateInput(source, target, mode string) error {
	//check if source and target are both provided
	if source == "" || target == "" {
		return fmt.Errorf("both source and target must be specifed")
	}

	//check if the source and target database name mentioned is valid
	if !isValidDatabase(source, supportedDatabases) {
		return fmt.Errorf("invalid source database type %s", source)
	}
	if !isValidDatabase(target, supportedDatabases) {
		return fmt.Errorf("invalid target database type %s", target)
	}

	//check if source and target are the same
	if source == target {
		return fmt.Errorf("source: %s and target: %s are the same ", source, target)
	}

	//validating migration modes
	validmodes := []string{"full", "incremental", "scheduled"}
	for _, v := range validmodes {
		if strings.EqualFold(v, mode) {
			return nil
		}
	}
	return fmt.Errorf("invalid mode: %s", mode)
}

func isValidDatabase(db string, slice []string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, db) {
			return true
		}
	}
	return false
}

// usage information
func printUsage() {
	fmt.Println("Usage Example:")
	fmt.Println(" ./binary --source=mysql --target=postgresql --mode=full")
	fmt.Println(" ./binary --source=mongodb --target=mysql --mode=full --workers=8 --backup")
	fmt.Println(" make run ARGS=\"--source=mysql --target=postgresql --mode=full\"")
	fmt.Println()
	fmt.Println("Available Options:")
	flag.PrintDefaults()
}

// creating appropriate database client based on type
func createDatabaseClient(dbType string, cfg *config.Config) database.DatabaseClient {
	switch strings.ToLower(dbType) {
	case "mysql":
		return database.NewMYSQLClientFromConfig(cfg)
	case "postgresql":
		return database.NewPostgreSQLClientFromConfig(cfg)
	case "mongodb":
		return database.NewMongoDBClientFromConfig(cfg)
	default:
		log.Fatalf("Unsupported database type, %s", dbType)
		return nil
	}
}

func main() {

	//defining CLI for user input
	sourceDB := flag.String("source", "", "Source Database type(mysql,postgresql,mongodb)")
	targetDB := flag.String("target", "", "Target Database type (mysql,postgresql,mongodb)")
	mode := flag.String("mode", "full", "Migration mode(full,incremental,scheduled)")
	//filetype := flag.String("filetype", "", "Format (csv,json,xml)")
	//filetype to be added later
	configPath := flag.String("config", "config.yaml", "Path to config file")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of worker goroutines for concurrent processing")
	batchsize := flag.Int("batch-size", 1000, "Batch size for data processing")
	concurrent := flag.Bool("concurrent", true, "Enable concurrent processing")
	validate := flag.Bool("validate", true, "Enable data validation")
	backup := flag.Bool("backup", false, "Create Backup before migration")

	//Advanced Options
	showVersion := flag.Bool("version", false, "Show version information")
	showHelp := flag.Bool("help", false, "Show detailed help information")
	listSnapshots := flag.Bool("list-snapshots", false, "List all available rollback snapshots")
	rollbackSnapshot := flag.String("rollback", "", "ROllback using specific snapshot ID")
	cleanupSnapshots := flag.String("cleanup-snapshots", "", "Cleanup snapshots older than duration(eg. '30d', '1h')")
	dryRun := flag.Bool("dry-run", false, "Performs validation and planning without actual migration")

	//custom usage function
	flag.Usage = func() {
		printUsage()
	}

	//parsing the user input
	flag.Parse()

	//Hanlding special commands first
	if *showVersion {
		fmt.Println("DataMigration Tool v1.0")
		fmt.Println("Built with Go", runtime.Version())
		fmt.Println("Support: MySQL, PostgreSQL, MongoDB")
		os.Exit(0)
	}

	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	//Loading config from config.yaml
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Error loading config %v", err)
	}

	fmt.Printf("Configuration loaded from %s \n", *configPath)

	//Handling rollback command
	if *rollbackSnapshot != "" {
		fmt.Printf("Initiating Rollback for Snapshot %s\n", *rollbackSnapshot)

		//creating a dummy engine for rollback
		targetClient := createDatabaseClient(*targetDB, cfg)
		if err := targetClient.Connect(); err != nil {
			log.Fatalf("Failed to connect to target database, %v", err)
		}
		defer targetClient.Close()

		dummyConfig := migration.MigrationConfig{TargetDb: *targetDB}
		engine := migration.NewMigrationEngine(dummyConfig, nil, targetClient)

		if err := engine.RollBackManager.RollBackMigration(*rollbackSnapshot); err != nil {
			log.Fatalf("Rollback Failed %v", err)
		}
		fmt.Printf("Rollback completed successful for snapshot %s\n", *rollbackSnapshot)
		os.Exit(0)
	}

	//handling snapshot listing
	if *listSnapshots {
		//creating a dummy engine to access rollback manager
		dummyConfig := migration.MigrationConfig{}
		engine := migration.NewMigrationEngine(dummyConfig, nil, nil)

		snapshots, err := engine.RollBackManager.ListSnapshots()
		if err != nil {
			log.Fatalf("Failed to list snapshot, %v", err)
		}

		if len(snapshots) == 0 {
			fmt.Println("No rollback snapshots, %v", err)
		} else {
			fmt.Println("Available Rollback Snapshot (%d):\n", len(snapshots))
			fmt.Println("ID		| Date		| Source->Target		| Status	| Tables")
			for _, snapshot := range snapshots {
				fmt.Printf("%-28s | %-19s | %-15s | %-8s | %d\n",
					snapshot.ID[:28],
					snapshot.Timestamp.Format("2025-05-11 15:04:50"),
					snapshot.SourceDB+"->"+snapshot.TargetDB,
					&snapshot.Status, len(snapshot.Tables))
			}
		}
		os.Exit(0)
	}

	//handling clean-up command
	if *cleanupSnapshots != "" {
		maxAge, err := time.ParseDuration(*cleanupSnapshots)
		if err != nil {
			log.Fatalf("Invalid duration format, %v", err)
		}

		dummyConfig := migration.MigrationConfig{}
		engime := migration.NewMigrationEngine(dummyConfig, nil, nil)

		if err := engime.RollBackManager.CleanupOldSnapshots(maxAge); err != nil {
			log.Fatalf("Cleanup failed %v", err)
		}
		fmt.Printf("Cleanup completed for snapshots older than %s\n", maxAge)
		os.Exit(0)
	}

	//validate input
	if err := validateInput(*sourceDB, *targetDB, *mode); err != nil {
		fmt.Println(" Validation Error: %v", err)
		printUsage()
		os.Exit(1)
	}

	fmt.Println("Input validated successfully")
	fmt.Printf("Starting Migration from %s to %s in %s mode", *sourceDB, *targetDB, *mode)

	if *dryRun {
		fmt.Printf("DRY RUN MODE: No actual data will be migrated\n ")
	}

	if *concurrent {
		fmt.Printf("Using concurrent processing with %d workers and batchsize %d", *workers, *batchsize)
	}

	if *backup {
		fmt.Println("Rollback snapshots enabled")
	}
	fmt.Println()

	//creating and connectinf source database client
	fmt.Printf("Connecting to Source database %s...\n", *sourceDB)
	sourceClient := createDatabaseClient(*sourceDB, cfg)

	if err := sourceClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to the source database, %v", err)
	}
	defer sourceClient.Close()
	fmt.Printf("Successfully connected to the source database %s", *sourceDB)

	//creating and connecting to the target database client
	fmt.Printf("COnnecting to the  Target database %s...\n", *targetDB)
	targetClient := createDatabaseClient(*targetDB, cfg)

	if err := targetClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to the target database, %v", err)
	}
	defer targetClient.Close()
	fmt.Printf("Successfully connected to the Target database %s", *targetDB)

	//Parsing SQL file or discovering collections for mongodb
	fmt.Println("Discovering tables and collections...")
	tables, err := getTablesOrCollections(*sourceDB, cfg, sourceClient)
	if err != nil {
		log.Fatalf("could not discover tables or collections, %v", err)
	}

	if len(tables) == 0 {
		log.Fatalf("no tables or collections found in the file,%v", err)
	}

	entityType := "tables"
	if strings.ToLower(*sourceDB) == "mongodb" {
		entityType = "collections"
	}
	fmt.Printf("Found %d %s, %v\n", len(tables), entityType, tables)

	//exiting early when it is dry run after discovery
	if *dryRun {
		fmt.Println("\n Dry Run Complete \n")
		fmt.Printf("Migrating %d %s from %s to %s \n", len(tables), entityType, *sourceDB, *targetDB)
		fmt.Printf("Run without --dry-run to perform actual migration \n")
		os.Exit(0)
	}

	//creating migration configuration
	migrationConfig := migration.MigrationConfig{
		Mode:         migration.MigrationMode(*mode),
		SourceDb:     *sourceDB,
		TargetDb:     *targetDB,
		Tables:       tables,
		Workers:      *workers,
		BatchSize:    *batchsize,
		Concurrent:   *concurrent,
		ValidateData: *validate,
		CreateBackup: *backup,
	}

	//creating and executing migration
	fmt.Printf("\n" + strings.Repeat("=", 60) + "\n")
	fmt.Printf("STARTING THE MIGRATION PROCESS")
	fmt.Printf(strings.Repeat("=", 60) + "\n")

	migrationEngine := migration.NewMigrationEngine(migrationConfig, sourceClient, targetClient)

	startTime := time.Now()

	result, err := migrationEngine.ExecuteMigration()
	if err != nil {
		log.Printf("Migration Failed, %v", err)
		if result != nil {
			result.Print()
		}

		//attempting rollback when failure occurs
		fmt.Printf("Attempting to rollback migration...")
		if rollbackErr := migrationEngine.RollBackManager; rollbackErr != nil {
			log.Printf("Rollback failed, %v", rollbackErr)
			fmt.Printf("Try Manual Rollback: ./binary --rollback=<snapshot_id>\n")
		} else {
			fmt.Printf("Rollback completed successfully\n")
		}
		os.Exit(1)
	}

	//printing success results
	result.Print()

	fmt.Printf("\n Migration Completed successfully")
	fmt.Printf("\n Migrated %d rows across %d tables from source database %s to target database %s in %v",
		result.TotalRowsMigrated, result.TotalTablesProcessed, *sourceDB, *targetDB, result.Duration)

	// fetch functionality of the source database tables
	fmt.Println("\n Fetching data from the source database...")
	var results []map[string]interface{}

	if *concurrent && len(tables) > 1 {
		fmt.Printf("Using Concurrent processing with %d workers ...\n", *workers)
		results, err = sourceClient.FetchAllDataConcurrently(tables, *workers)
	} else {
		fmt.Printf("Using sequential processing...\n")
		results, err = sourceClient.FetchAllData(tables)
	}

	if err != nil {
		log.Fatalf("failed to fetch data %v", err)
	}
	fmt.Printf("Fetched %d rows of data:", len(results))

	//Handling target database
	if *targetDB != "" {
		fmt.Printf("Preparing to migrate data to %s.. ", *targetDB)

		var targetClient database.DatabaseClient

		switch strings.ToLower(*targetDB) {
		case "mysql":
			targetClient = database.NewMYSQLClientFromConfig(cfg)
		case "postgresql":
			targetClient = database.NewPostgreSQLClientFromConfig(cfg)
		case "mongodb":
			targetClient = database.NewMongoDBClientFromConfig(cfg)
		default:
			log.Fatalf("unsupported database target type %s", *targetDB)
		}

		if err := targetClient.Connect(); err != nil {
			log.Fatalf("failed to connect to the target %s database, %v", *targetDB, err)
		}
		defer targetClient.Close()

		fmt.Printf("Successfully connected to the target %s database", *targetDB)

		fmt.Printf("Importing Data to target database")

		if *concurrent && len(results) > *batchsize {
			fmt.Printf("Using Concurrent batch processing with batch size %d...\n", *batchsize)
			if err = targetClient.ImportDataConcurrently(results, *batchsize); err != nil {
				log.Fatalf("Failed to import data concurrently: %v", err)
			}
		} else {
			fmt.Println("Using sequential import...")
			if err = targetClient.ImportData(results); err != nil {
				log.Fatalf("Failed to import data: %v", err)
			}
		}

		if err != nil {
			log.Fatalf("failed to import data, %v", err)
		}
		fmt.Println("Data Migration completed successfully !!!")
	}
	fmt.Println("Migration Process completed!!")

	// Print success results
	fmt.Printf("\n" + strings.Repeat("=", 60) + "\n")
	fmt.Printf("🎉 MIGRATION COMPLETED SUCCESSFULLY!\n")
	fmt.Printf(strings.Repeat("=", 60) + "\n")

	result.Print()

	// Success summary
	totalTime := time.Since(startTime)
	avgSpeed := float64(result.TotalRowsMigrated) / totalTime.Seconds()

	fmt.Printf("\n📊 Performance Summary:\n")
	fmt.Printf("   ⚡ Speed: %.0f rows/second\n", avgSpeed)
	fmt.Printf("   📈 Throughput: %.0f rows/minute\n", avgSpeed*60)
	fmt.Printf("   🏆 Efficiency: %.1f tables/minute\n", float64(result.TotalTablesProcessed)/totalTime.Minutes())

	if result.TotalRowsMigrated > 100000 {
		fmt.Printf("   🚀 High-volume migration completed!\n")
	}

	// Cleanup suggestions
	if *backup {
		fmt.Printf("\n💡 Management Commands:\n")
		fmt.Printf("   📋 List snapshots: ./binary --list-snapshots\n")
		fmt.Printf("   🧹 Cleanup old snapshots: ./binary --cleanup-snapshots=30d\n")
	}

	fmt.Printf("\n✨ Migration completed successfully in %v\n", totalTime)
	fmt.Printf("🎯 Ready for production use!\n")
}

// helper function for handling mongodb parsing logic and SQL table discovery
func getTablesOrCollections(sourceDB string, cfg *config.Config, sourceClient database.DatabaseClient) ([]string, error) {
	switch strings.ToLower(sourceDB) {
	case "mongodb":
		//for mongodb, discover collections from database
		if mongoClient, ok := sourceClient.(*database.MongoDBClient); ok {
			collections, err := mongoClient.GetCollectionNames()
			if err != nil {
				return nil, fmt.Errorf("failed to get MongoDB collections, %v", err)
			}
			if len(collections) == 0 {
				return nil, fmt.Errorf("no collections found in mongodb database")
			}
			return collections, nil
		}
		return nil, fmt.Errorf("failed to cast to MongoDB client")
	case "mysql", "postgresql":
		//for sql databases, parse SQL files
		if cfg.SQLFilePath == "" {
			return nil, fmt.Errorf("SQL file path not specified in the configuration")
		}
		parser := &database.SQLParser{}
		tables, err := parser.ParseSQLFiles(cfg.SQLFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SQL file %s, %v", cfg.SQLFilePath, err)
		}
		if len(tables) == 0 {
			return nil, fmt.Errorf("no tables found in SQL file %s,%v", cfg.SQLFilePath, err)
		}

		return tables, nil
	default:
		return nil, fmt.Errorf("unsupported database type %s", sourceDB)
	}
}

//ToDo: check on fetch block
