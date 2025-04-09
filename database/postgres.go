package database

type PostgresMigration struct {
}

func ConnectPostgres() error {
	return nil
}

func (p *PostgresMigration) InsertData(data []map[string]interface{}) error {
	return nil
}
