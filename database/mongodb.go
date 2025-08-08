package database

import (
	"context"
	"fmt"

	"github.com/SusheelSathyaraj/DataMigrationTool/config"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDBClient struct {
	URI      string
	DBName   string
	Client   *mongo.Client
	Database *mongo.Database
	ctx      context.Context
}

// creating a new MongoDbClient using manual parameters
func NewMongoDBClient(uri, dbname string) *MongoDBClient {
	return &MongoDBClient{
		URI:    uri,
		DBName: dbname,
		ctx:    context.Background(),
	}
}

// creating a new MongoDBClient using config
func NewMongoDBClientFromConfig(cfg *config.Config) *MongoDBClient {
	//building uri from config
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d/%s",
		cfg.MongoDB.User,
		cfg.MongoDB.Password,
		cfg.MongoDB.Host,
		cfg.MongoDB.Port,
		cfg.MongoDB.DBName,
	)

	return &MongoDBClient{
		URI:    uri,
		DBName: cfg.MongoDB.DBName,
		ctx:    context.Background(),
	}
}
