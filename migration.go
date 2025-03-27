package main

// interface to manage the migration
type Migration interface {
	Connect() error                                  //establish conection with the database
	FetchData() ([]map[string]interface{}, error)    //retreive data from the source database
	MigrateData(data []map[string]interface{}) error //send data to the target databse
}
