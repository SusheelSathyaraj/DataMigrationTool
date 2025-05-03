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

	//var db *sql.DB

	//checking the connection to database
	fmt.Printf("\n Attempting to connect to %s database...", *sourceDB)

	var sourceClient database.DatabaseClient

	switch strings.ToLower(*sourceDB) {
	case "mysql":
		sourceClient = database.NewMYSQLClientFromConfig(cfg)
	case "postgresql":
		//TO DO: sourceClient = database.NewPostgreSQLFromConfig(cfg)
		log.Fatal("PostgresQL not yet implemented")
	default:
		log.Fatalf("Unsupported source database type, %s", *sourceDB)
	}

	if err := sourceClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to %s Database, %v", *sourceDB, err)
	}

	//Using type ascertion to get correct client type
	mysqlClient, ok := sourceClient.(*database.MySQLClient)
	if !ok {
		log.Fatalf("Failed to cast source client to expected type")
	}

	defer mysqlClient.Close()

	fmt.Printf("Successfully connected to %s database", *sourceDB)

	//Parsing SQL file
	fmt.Println("Fetching data from source database...")
	parser := &database.SQLParser{}
	tables, err := parser.ParseSQLFiles(cfg.SQLFilePath)
	if err != nil {
		log.Fatalf("could not parse the SQL file, %v", err)
	}

	if len(tables) == 0 {
		log.Fatalf("No tables found in the SQL file")
	}

	fmt.Printf("Found %d tables in %v ", len(tables), tables)

	// fetch functionality of the mysql database tables
	fmt.Println("\n Fetching data from the source database...")
	results, err := mysqlClient.FetchAllData(tables)
	if err != nil {
		log.Fatalf("failed to fetch data %v", err)
	}
	fmt.Printf("Fetched %d rows of data:", len(results))

	//Handling target database
	if *targetDB != "" {
		fmt.Printf("Preparing to migrate data to %s.. ", *targetDB)
		switch strings.ToLower(*targetDB) {
		case "postgresql":
			//TO DO import logic
			fmt.Println("Postgres import not  yet implemented")
		case "mongodb":
			//TO Do import logic
			fmt.Println("MongoDb logic not yet implemented")
		default:
			log.Fatalf("unsupported database target type %s", *targetDB)
		}
	}
	fmt.Println("Migration Process completed!!")
}
