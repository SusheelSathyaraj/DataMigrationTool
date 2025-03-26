package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
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
	return nil
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
	mode := flag.String("mode", "", "Migration mode(full,incremental,scheduled)")
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
}
