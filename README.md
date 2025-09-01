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

### Test Coverage
- **Unit Tests**: Core functionality and business logic
- **Integration Tests**: Database connectivity and operations
- **Benchmark Tests**: Performance Validation
- **Mock Test**: Isolated component testing 

## Perfomrance Characteristics
- **Throughput**: 50,000+ rows/sec (depends on hardware and network)
- **Memory Usage**: Configurable batch processing for large datasets
- **Concurrency**: Scales with available CPU cores
- **Network Efficient**: Optimized connection pooling and batching

## Error Handling and Recovery
- **Graceful degradation** with detailed error reporting
- **Automatic retry** mechanisms for transient failures
- **Transaction rollback** on critical errors
- **Comprehensive logging** for troubleshooting
- **Health checks** with status monitoring

## Contributing
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Write comprehensive tests for new features
- Follow Go best practices and formatting
- Update documentation for API changes
- Ensure backward compatibility

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Roadmap

### Upcoming Features
- [ ] **Incremental Migration**: Delta sync with timestamp tracking
- [ ] **Scheduled Migration**: Cron-based automated migrations  
- [ ] **CSV Import/Export**: File-based data transfer
- [ ] **REST API**: HTTP interface for remote management
- [ ] **Web Dashboard**: Browser-based monitoring and control
- [ ] **Docker Support**: Containerized deployment
- [ ] **Cloud Integration**: AWS RDS, Google Cloud SQL support

### Performance Improvements
- [ ] **Streaming Processing**: Memory-constant large dataset handling
- [ ] **Compression**: Reduced network overhead
- [ ] **Parallel Schema Creation**: Faster initial setup
- [ ] **Advanced Caching**: Improved metadata handling

**Note**: This tool is designed for production environments and handles enterprise-scale data migrations. Always test migrations in a development environment before running in production.