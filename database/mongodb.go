package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/SusheelSathyaraj/DataMigrationTool/config"
	"go.mongodb.org/mongo-driver/bson"
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

// Executing the query, MongoDB uses aggregation pipeline
func (m *MongoDBClient) ExecuteQuery(query string) (*sql.Rows, error) {
	//MongoDb does not use SQL, this is just a plcaeholder for interface compliance
	//In practise, convert the SQL to MongoDB aggregation pipeline
	return nil, fmt.Errorf("ExecuteQuery is not implemented for MongoDb, use MongoDB- specific methods")
}

// fetching data from all specified collections
func (m *MongoDBClient) FetchAllData(collections []string) ([]map[string]interface{}, error) {
	if m.Database == nil {
		return nil, fmt.Errorf("database connection cannot be established")
	}

	var allResults []map[string]interface{}

	for _, collectionName := range collections {
		collection := m.Database.Collection(collectionName)

		//creating context with timeout
		ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)

		//finding all documents
		cursor, err := collection.Find(ctx, bson.M{})
		if err != nil {
			cancel()
			return nil, fmt.Errorf("error fetching data from collection %s,%v", collectionName, err)
		}

		//Decoding all documents
		var collectionResult []map[string]interface{}
		if err := cursor.All(ctx, &collectionResult); err != nil {
			cursor.Close(ctx)
			cancel()
			return nil, fmt.Errorf("error decoding data from collection %s, %v", collectionName, err)
		}

		cursor.Close(ctx)
		cancel()

		//Adding collection info into each document
		for i := range collectionResult {
			collectionResult[i]["_source_table"] = collectionName
		}

		allResults = append(allResults, collectionResult...)
		fmt.Printf("Fetched %d documents from collection %s", len(collectionResult), collectionName)
	}
	return allResults, nil
}

// importing data into the mongodb collections
func (m *MongoDBClient) ImportData(data []map[string]interface{}) error {
	if m.Database == nil {
		return fmt.Errorf("database connection cannot be establshed")
	}
	if len(data) == 0 {
		return fmt.Errorf("no data to import")
	}

	//grouping data by collection
	collectionData := make(map[string][]interface{})
	for _, row := range data {
		collectionName, ok := row["_source_table"].(string)
		if !ok {
			return fmt.Errorf("row missing source table info")
		}

		//removing _source_table field before inserting
		document := make(map[string]interface{})
		for key, value := range row {
			if key != "_source_table" {
				document[key] = value
			}
		}
		collectionData[collectionName] = append(collectionData[collectionName], document)
	}
	//inserting data into  each collection
	for collectionName, documents := range collectionData {
		if len(documents) == 0 {
			continue
		}
		collection := m.Database.Collection(collectionName)

		//creating context with timeout
		ctx, cancel := context.WithTimeout(m.ctx, 60*time.Second)

		//inserting many documents
		result, err := collection.InsertMany(ctx, documents)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to insert data into the collection %s:%v", collectionName, err)
		}

		cancel()
		fmt.Printf("Successfully imported %d documents into collection %s", len(result.InsertedIDs), collectionName)
	}
	return nil
}

// fetching data concurrently frmo multiple collections using workerpool
func (m *MongoDBClient) FetchAllDataConcurrently(collections []string, numWorkers int) ([]map[string]interface{}, error) {
	if numWorkers <= 0 {
		numWorkers = 4 //Default number of workers
	}
	//using workerpool functionality
	return ProcessTablesWithWorkerPool(m, collections, numWorkers)
}

// importing data concurrently usig batch processing
func (m *MongoDBClient) ImportDataConcurrently(data []map[string]interface{}, batchSize int) error {
	if batchSize <= 0 {
		batchSize = 1000 //Default  batchsize
	}
	processor := NewBatchProcessor(batchSize)

	return processor.ProcessInBatches(data, m.ImportData)
}

//Helper functions

// retrieving all collection names from the DB
func (m *MongoDBClient) GetCollectionNames() ([]string, error) {
	if m.Database == nil {
		return nil, fmt.Errorf("database connection not established")
	}
	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	return m.Database.ListCollectionNames(ctx, bson.M{})
}

// creating an index on a collection
func (m *MongoDBClient) CreateIndex(collectionName string, keys map[string]int) error {
	if m.Database == nil {
		return fmt.Errorf("database connection not established")
	}
	collection := m.Database.Collection(collectionName)

	//converting keys to bson document
	indexKeys := bson.M{}
	for key, order := range keys {
		indexKeys[key] = order
	}

	indexModel := mongo.IndexModel{
		Keys: indexKeys,
	}

	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("failed to create index on collection %s:%v", collectionName, err)
	}

	fmt.Printf("Created index on collection %s with keys %v", collectionName, keys)
	return nil
}

// to counting documents in a collection
func (m *MongoDBClient) CountDocuments(collectionName string, filter map[string]interface{}) (int64, error) {
	if m.Database == nil {
		return 0, fmt.Errorf("database connection not established")
	}
	collection := m.Database.Collection(collectionName)

	//converting filter to bson
	bsonFilter := bson.M{}
	for key, value := range filter {
		bsonFilter[key] = value
	}

	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	return collection.CountDocuments(ctx, bsonFilter)
}

// MongoDB data type conversion helpers
func convertToMongoType(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	//handling different go types and converting compatible mongodb types
	switch v := value.(type) {
	case string:
		return v
	case int, int32, int64:
		return v
	case float32, float64:
		return v
	case bool:
		return v
	case []byte:
		return string(v)
	case time.Time:
		return v
	default:
		//for complex types coverting to string
		return fmt.Sprintf("%v", v)
	}
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

// handling mongodb collection discovery
type MongoCollectionParser struct{}

// discovering collections directly from MongoDB
func (p *MongoCollectionParser) ParseCollectionsFromDatabase(client *MongoDBClient) ([]string, error) {
	return client.GetCollectionNames()
}

// converting SQL table names to MongoDB collections
// for migrating from sql to mongodb
func (p *MongoCollectionParser) ParseCollectionsFromSQL(sqlFilePath string) ([]string, error) {
	//reusing the existing sql parser and treating tables as collections
	sqlParser := &SQLParser{}
	tableNames, err := sqlParser.ParseSQLFiles(sqlFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL file for collection names: %v", err)
	}

	//converting table names to collection names
	collections := make([]string, len(tableNames))
	for i, tableName := range tableNames {
		// Convert to MongoDB collection naming convention (optional)
		// e.g., "user_profiles" stays "user_profiles" or becomes "userProfiles"
		collections[i] = strings.ToLower(tableName)
	}
	return collections, nil
}
