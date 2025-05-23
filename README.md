# Overview

The Data Migration Tool is a flexible and efficient solution for migrating data across different databases and formats. It supports seamless transfers between `MySQL`, `PostgreSQL`, and `MongoDB`, along with `CSV-to-database` synchronization. This tool ensures data integrity, real-time progress tracking, and robust error handling, making it ideal for enterprise-level migrations.

# Features

    Multi-Database Support: 
      Supports migration across MySQL, PostgreSQL, and MongoDB.
      Extendable to other databases via additional connectors.

    Migration Modes:
      Full Migration: Migrate entire datasets from source to target.
      Incremental Migration: Sync only the modified data.
      Scheduled Migration: Set migrations to run at specific intervals(cron schedules).
    
    Advanced Monitoring and Logging:
      Real-time Progress Monitoring: Track the progress of your migration through a web-based dashboard or CLI.
      Error Handling and Logging: Provides detailed logs and error recovery mechanisms to ensure smooth migration.
      Validation: Pre- and post-migration validation to ensure data integrity.
    
    Secure Migration: 
      Encryption to protect sensitive data during migration. 
      Safe Logging to avoid exposure of confidential details.

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
