package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/SusheelSathyaraj/DataMigrationTool/config"

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

	//parsing the user input
	flag.Parse()

	//Loading config from config.yaml
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Error loading config %v", err)
	}

	//validate input
	if err := validateInput(*sourceDB, *targetDB, *mode); err != nil {
		fmt.Println("Error:", err)
		flag.Usage()
		os.Exit(1)
	}
	fmt.Println("Input validated successfully")
	fmt.Printf("Starting Migration from %s to %s in %s mode", *sourceDB, *targetDB, *mode)

	if *concurrent {
		fmt.Printf("Using concurrent processing with %d workers and batchsize %d", *workers, *batchsize)
	}

	//checking the connection to database
	fmt.Printf("\n Attempting to connect to %s database...", *sourceDB)

	var sourceClient database.DatabaseClient

	switch strings.ToLower(*sourceDB) {
	case "mysql":
		sourceClient = database.NewMYSQLClientFromConfig(cfg)
	case "postgresql":
		sourceClient = database.NewPostgreSQLClientFromConfig(cfg)
	case "mongodb":
		sourceClient = database.NewMongoDBClientFromConfig(cfg)
	default:
		log.Fatalf("Unsupported source database type, %s", *sourceDB)
	}

	if err := sourceClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to %s Database, %v", *sourceDB, err)
	}
	defer sourceClient.Close()
	fmt.Printf("successfully connected to the %s database", *sourceDB)

	//Parsing SQL file or discovering collections for mongodb
	fmt.Println("Discovering tables and collections...")
	tables, err := getTablesOrCollections(*sourceDB, cfg, sourceClient)
	if err != nil {
		log.Fatalf("could not discover tables or collections, %v", err)
	}

	if len(tables) == 0 {
		log.Fatalf("no tables or collections found in the file,%v", err)
	}

	if strings.ToLower(*sourceDB) == "mongodb" {
		fmt.Printf("Found %d collections : %v", len(tables), tables)
	} else {
		fmt.Printf("Found %d tables:: %v", len(tables), tables)
	}

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
}

// helper function for handling mongodb parsing logic
func getTablesOrCollections(sourceDB string, cfg *config.Config, sourceClient database.DatabaseClient) ([]string, error) {
	switch strings.ToLower(sourceDB) {
	case "mongodb":
		//for mongodb, discover collections from database
		if mongoClient, ok := sourceClient.(*database.MongoDBClient); ok {
			return mongoClient.GetCollectionNames()
		}
		return nil, fmt.Errorf("failed to cast to MongoDB client")
	case "mysql", "postgresql":
		//for sql databases, parse SQL files
		parser := &database.SQLParser{}
		return parser.ParseSQLFiles(cfg.SQLFilePath)
	default:
		return nil, fmt.Errorf("unsupported database type %s", sourceDB)
	}
}
