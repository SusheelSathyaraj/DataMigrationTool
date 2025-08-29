# Overview

A high-performance, enterprise-grade CLI tool for seamless data migration between MySQL, PostgreSQL, and MongoDB databases. Built with Go's powerful concurrency model for handling large-scale data transfers with minimal downtime.

# Features

###  **Multi-Database Support** 
- **Source & Target**: MySQL, PostgreSQL, MongoDB
- **Cross-platform migrations** (MySQL → PostgreSQL, MongoDB → MySQL, etc.)
- **Intelligent schema handling** with automatic type conversion

###  **Migration Modes**
- **Full Migration**: Complete dataset transfer from source to target
- **Incremental Migration**: Sync only modified data (future scope)
- **Scheduled Migration**: Automated recurring migrations (future scope)
    
### **Enterprise Grade Reliability**
- **Pre & Post-migration validation** with data integrity checks
- **Real-time progress monitoring** with ETA calculations
- **Comprehensive error handling** and recovery mechanisms
- **Detailed logging** with structured output
- **Rollback capabilities** for failed migrations

### **High-Performance Architecture**
- **Concurrent processing** with configurable worker pools
- **Batch processing** for memory-efficient large dataset handling
- **Connection pooling** for optimal database performance
- **Progress tracking** with live metrics and monitoring

### **Production Ready Features**
- **Data validation** with type checking and integrity verification
- **Health monitoring** with real-time status reporting
- **Comprehensive testing** including unit, integration, and benchmark tests
- **Clean architecture** with modular, extensible design

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/data-migration-tool.git
cd data-migration-tool

# Install dependencies
go mod tidy

# Build the binary
make build
```

### Configuration

Create a `config.yaml` file:

```yaml
mysql:
  host: "localhost"
  port: 3306
  user: "root"
  password: "password"
  dbname: "source_db"

postgresql:
  host: "localhost"
  port: 5432
  user: "postgres"
  password: "password"
  dbname: "target_db"

mongodb:
  host: "localhost"
  port: 27017
  user: "admin"
  password: "password"
  dbname: "target_db"

sqlfile_path: "/path/to/schema.sql"
```

## Command Line Options

| Flag           | Description                    | Default       | Example                            |
|----------------|--------------------------------|---------------|------------------------------------|
| `--source`     | Source database type           | -             | `mysql`, `postgresql`, `mongodb`   |
| `--target`     | Target database type           | -             | `mysql`, `postgresql`, `mongodb`   |
| `--mode`       | Migration mode                 | `full`        | `full`, `incremental`, `scheduled` |
| `--config`     | Configuration file path        | `config.yaml` | `./my-config.yaml`                 |
| `--workers`    | Number of concurrent workers   | CPU count     | `8`                                |
| `--batch-size` | Batch size for processing      | `1000`        | `5000`                             |
| `--concurrent` | Enable concurrent processing   | `true`        | `false`                            |
| `--validate`   | Enable data validation         | `true`        | `false`                            |
| `--backup`     | Create backup before migration | `false`       | `true`                             |

## Architecture

### Project Structure

```
DataMigrationTool/
├── config/           # Configuration management
├── database/         # Database clients and interfaces
├── migration/        # Migration engine and logic
├── monitoring/       # Progress tracking and logging
├── validation/       # Data validation and integrity checks
└── tests/            # Comprehensive test suite
```

### Key Components

- **Migration Engine**: Orchestrates the entire migration process
- **Database Clients**: Abstracted interfaces for each database type
- **Validation System**: Pre/post-migration data integrity checks
- **Progress Monitor**: Real-time tracking with metrics and ETA
- **Worker Pool**: Concurrent processing with configurable parallelism
- **Batch Processor**: Memory-efficient handling of large datasets

## Real-time Monitoring

The tool provides comprehensive real-time monitoring:

```
[14:32:15] Progress: 67.3% (134,560/200,000 rows, 3/5 tables) | Speed: 1,247 rows/sec | ETA: 52s | Current: user_profiles

=== Migration Summary ===
Total Duration: 2m34s
Rows Processed: 200,000 / 200,000 (100.0%)
Tables Processed: 5 / 5
Average Speed: 1,298 rows/sec (77,880 rows/min)
Tables per Minute: 1.9
========================
```

## Testing

### Run Tests
```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package tests
go test ./validation -v

# Run benchmarks
go test -bench=. ./...
```


# Tech Stack

    Backend: 
        Golang (Go)
        Database drivers: mysql, pgx (PostgreSQL), and mongo-go-driver for MongoDB.
        Logging: Logrus or Zap for structured logging.
        Concurrency: Golang’s goroutines for handling large data migrations efficiently.
    Frontend (Optional): TypeScript with React (for web-based interface).
        API Calls: Axios or Fetch.
        UI Libraries: Material-UI or Ant Design.
    Others:
        Docker: For containerized deployment.
        CI/CD: GitHub Actions for automated testing and deployment.
        Kubernetes: For scalable deployment (optional).

How It Works

    Setup: Define the source and target databases in the configuration file or via the dashboard.
    Run Migration: Initiate a full, incremental, or scheduled migration.
    Monitor: Track progress in real-time and view logs in case of errors.
    Validation: Post-migration, run validation scripts to ensure all data has been transferred correctly.

# Installation

Prerequisites

    Go 1.18+
    TypeScript (if using frontend)
    Docker (optional for containerization)
    MySQL, PostgreSQL, or MongoDB installed locally or available via a connection URL.

# Steps:

Clone the Repository:

    bash
    git clone https://github.com/yourusername/data-migration-tool.git
    cd data-migration-tool

Set Up Environment Variables: Create a config.yaml file in the project root and configure your database credentials:

    bash
    DB_SOURCE=mysql
    DB_SOURCE_URI=username:password@tcp(localhost:3306)/source_db
    DB_TARGET=postgresql
    DB_TARGET_URI=username:password@tcp(localhost:5432)/target_db

Run Backend: To build and run the Golang backend:

    bash
    go mod tidy
    go run main.go

Optional: Run Frontend: If using the frontend, navigate to the frontend/ folder and run:

    bash
    cd frontend
    npm install
    npm start

Docker Setup (Optional): To run the application inside a Docker container:

    bash
    docker-compose up --build

# Usage

## CLI Mode:

You can run migrations directly via the CLI:

    bash
    go run main.go --source=mysql --target=postgresql --mode=full
    (full being the default mode)
    (OR)
    make run ARGS="--source=mysql --target=postgresql --mode=full"
    make test ARGS="--source=mysql --target=postgresql --mode=full"
    (to run all tests)

    Source: Specify the source database (mysql, postgresql, mongodb).
    Target: Specify the target database (mysql, postgresql, mongodb).
    Mode: Choose from (full, incremental, scheduled).

## Web Interface (Optional):

    Open the dashboard in your browser to track real-time progress, initiate new migrations, and configure settings.

## REST API (Optional)

This tool also provides a REST API to trigger and monitor migrations.

### **Endpoints**
| HTTP Method | Endpoint          | Description |
|------------|------------------|-------------|
| `POST`     | `/migrate`        | Start a migration |
| `GET`      | `/status/{id}`     | Check migration status |
| `GET`      | `/logs/{id}`       | View migration logs |
| `DELETE`   | `/cancel/{id}`     | Cancel a migration |

### **Example Usage**

#### **Start a Migration**
```sh
curl -X POST http://localhost:8080/migrate \
     -H "Content-Type: application/json" \
     -d '{"source": "mysql", "target": "postgresql", "mode": "full"}'


Configuration

You can customize the migration configuration in the config.json file:

json

    {
     "source": "mysql",
     "source_uri": "username:password@tcp(localhost:3306)/source_db",
     "target": "postgresql",
     "target_uri": "username:password@tcp(localhost:5432)/target_db",
     "migration_mode": "full",
     "schedule": "0 2 * * *" // for scheduled migrations (cron format)
    }

# Logging and Error Handling

All logs are stored in the /logs directory and are rotated daily to ensure easy tracking. Errors are logged with detailed stack traces to help identify issues during migration.

# Testing

Unit tests are provided for the critical components of the migration process. To run tests:

    bash
    go test ./...

# Contributing

Contributions are welcome! Please follow these steps:

    Fork the repo.
    Create a new feature branch (git checkout -b feature/my-feature).
    Commit your changes (git commit -m 'Add some feature').
    Push to the branch (git push origin feature/my-feature).
    Open a Pull Request.

License

This project is licensed under the MIT License - see the LICENSE file for details.
