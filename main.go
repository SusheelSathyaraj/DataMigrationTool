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
	//Loading config from config.yaml
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Error loading config %v", err)
	}

	//defining CLI for user input
	sourceDB := flag.String("source", "", "Source Database type(mysql,postgresql,mongodb)")
	targetDB := flag.String("target", "", "Target Database type (mysql,postgresql,mongodb)")
	mode := flag.String("mode", "full", "Migration mode(full,incremental,scheduled)")
	//filetype := flag.String("filetype", "", "Format (csv,json,xml)")
	//filetype to be added later

	//parsing the user input
	flag.Parse()

	//validate input
	if err := validateInput(*sourceDB, *targetDB, *mode); err != nil {
		fmt.Println("Error:", err)
		flag.Usage()
		os.Exit(1)
	}
	fmt.Println("Input validated successfully")
	fmt.Printf("Starting Migration from %s to %s in %s mode", *sourceDB, *targetDB, *mode)

	//checking the connection to mysql database
	fmt.Println("\n Attempting to connect to MySQL database...")
	db, err := database.ConnectMySQL(cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port, cfg.MySQL.DBName)
	if err != nil {
		log.Fatalf("connection failed %v", err)
	}
	defer db.Close()
	fmt.Println("Connection Successful")

	//checking the fetch functionality of the mysql database
	fmt.Println("\n Fetching data from the mysql database...")
	data, err := database.FetchData(db, cfg.FilePath)
	if err != nil {
		log.Fatalf("failed to fetch data %v", err)
	}
	fmt.Printf("Fetched data is: %v", data)

}
