package main

import (
	"flag"
	"fmt"
	"log"
	"os"
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

	//checking the connection to database
	fmt.Printf("\n Attempting to connect to %s database...", *sourceDB)

	var sourceClient database.DatabaseClient

	switch strings.ToLower(*sourceDB) {
	case "mysql":
		sourceClient = database.NewMYSQLClientFromConfig(cfg)
	case "postgresql":
		sourceClient = database.NewPostgreSQLClientFromConfig(cfg)
	default:
		log.Fatalf("Unsupported source database type, %s", *sourceDB)
	}

	if err := sourceClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to %s Database, %v", *sourceDB, err)
	}
	defer sourceClient.Close()
	fmt.Printf("successfully connected to the %s database", *sourceDB)

	//Parsing SQL file
	fmt.Println("Fetching data from source database...")
	parser := &database.SQLParser{}
	tables, err := parser.ParseSQLFiles(cfg.SQLFilePath)
	if err != nil {
		log.Fatalf("could not parse the SQL file, %v", err)
	}

	if len(tables) == 0 {
		log.Fatalf("no tables found in the SQL file,%v", err)
	}

	fmt.Printf("Found %d tables, %v", len(tables), tables)

	// fetch functionality of the mysql database tables
	fmt.Println("\n Fetching data from the source database...")
	results, err := sourceClient.FetchAllData(tables)
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
			//TO Do import logic
			fmt.Println("MongoDb logic not yet implemented")
			return
		default:
			log.Fatalf("unsupported database target type %s", *targetDB)
		}

		if err := targetClient.Connect(); err != nil {
			log.Fatalf("failed to connect to the target %s database, %v", *targetDB, err)
		}
		defer targetClient.Close()

		fmt.Println("Successfully connected to the target %s database", *targetDB)

		//Import data to the target database
		fmt.Println("Importing data to the target database")
		err = targetClient.ImportData(results)
		if err != nil {
			log.Fatalf("failed to import data, %v", err)
		}
		fmt.Println("Data Migration completed successfully !!!")
	}
	fmt.Println("Migration Process completed!!")
}
