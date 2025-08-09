package database

import (
	"context"
	"fmt"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

// connecting to mongoDB
func (m *MongoDBClient) Connect() error {
	//setting client options
	clientOptions := options.Client().ApplyURI(m.URI)

	//setting timeout for connection
	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	//connecting to mongodb
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to mongodb: %v", err)
	}

	//checking connection
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	m.Client = client
	m.Database = client.Database(m.DBName)

	fmt.Println("Successfully connected to mongoDB")
	return nil
}

// closing the mongodb connection
func (m *MongoDBClient) Close() error {
	if m.Client != nil {
		ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
		defer cancel()
		return m.Client.Disconnect(ctx)
	}
	return nil
}

// backward compatiblty functions
func ConnectMongoDB(uri, dbname string) (*MongoDBClient, error) {
	client := NewMongoDBClient(uri, dbname)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to the mongodb:%v", err)
	}
	return client, nil
}

func ConnectMongoDBFromConfig(cfg *config.Config) (*MongoDBClient, error) {
	client := NewMongoDBClientFromConfig(cfg)
	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to the mongodb:%v", err)
	}
	return client, nil
}
