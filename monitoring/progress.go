package monitoring

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// struct tracking migration process with thread safe operations
type ProcessTracker struct {
	mu              sync.RWMutex
	totalRows       int64
	processedRows   int64
	totalTables     int
	processedTables int
	startTime       time.Time
	currentTable    string
	errors          []string
	lastUpdate      time.Time
}

// struct holding migration metrics
type MigrationMetrics struct {
	TotalRows         int64         `json:"total_rows"`
	ProcessedRows     int64         `json:"processed_rows"`
	TotalTables       int           `json:"total_tables"`
	ProcessedTables   int           `json:"processed_tables"`
	RowsPerSecond     float64       `json:"rows_per_second"`
	TablesPerMinute   float64       `json:"tables_per_minute"`
	EstimatedTimeLeft time.Duration `json:"estimated_time_left"`
	ElapsedTime       time.Duration `json:"elapsed_time"`
	CurrentTable      string        `json:"current_table"`
	ErrorCount        int           `json:"error_count"`
	ProgressPercent   float64       `json:"progress_percent"`
}

// creating a new progress tracker
func NewProgressTracker(totalRows int64, totalTables int) *ProcessTracker {
	return &ProcessTracker{
		totalRows:   totalRows,
		totalTables: totalTables,
		startTime:   time.Now(),
		lastUpdate:  time.Now(),
		errors:      make([]string, 0),
	}
}

// updating the number of processed rows (threadsafe)
func (pt *ProcessTracker) UpdateProgress(rowsProcessed int64) {
	atomic.AddInt64(&pt.processedRows, rowsProcessed)
	pt.mu.Lock()
	pt.lastUpdate = time.Now()
	pt.mu.Unlock()
}

// updating the currently processing table
func (pt *ProcessTracker) SetCurrentTable(tableName string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.currentTable = tableName
}

// marks the table as completed
func (pt *ProcessTracker) CompletedTable() {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.processedTables++
}

// adding an error to the error list
func (pt *ProcessTracker) AddError(err string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.errors = append(pt.errors, fmt.Sprintf("[%s]%s", time.Now().Format("22:11:15"), err))
}

// returning current migration matrics
func (pt *ProcessTracker) GetMetrics() MigrationMetrics {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	processedRows := atomic.LoadInt64(&pt.processedRows)
	elapsedTime := time.Since(pt.startTime)

	var progressPercent float64
	if pt.totalRows > 0 {
		progressPercent = float64(processedRows) / float64(pt.totalRows) * 100
	}

	var rowsPerSecond float64
	if elapsedTime.Seconds() > 0 {
		rowsPerSecond = float64(processedRows) / (elapsedTime).Seconds()
	}

	var tablesPerMinute float64
	if elapsedTime.Minutes() > 0 {
		tablesPerMinute = float64(pt.processedTables) / elapsedTime.Minutes()
	}

	var estimatedTimeLeft time.Duration
	if rowsPerSecond > 0 && pt.totalRows > processedRows {
		remainingRows := pt.totalRows - processedRows
		estimatedTimeLeft = time.Duration(float64(remainingRows)/rowsPerSecond) * time.Second
	}

	return MigrationMetrics{
		TotalRows:         pt.totalRows,
		ProcessedRows:     pt.processedRows,
		TotalTables:       pt.totalTables,
		ProcessedTables:   pt.processedTables,
		RowsPerSecond:     rowsPerSecond,
		TablesPerMinute:   tablesPerMinute,
		EstimatedTimeLeft: estimatedTimeLeft,
		ElapsedTime:       elapsedTime,
		CurrentTable:      pt.currentTable,
		ErrorCount:        len(pt.errors),
		ProgressPercent:   progressPercent,
	}
}

// returning the most recent errors(up to limit)
func (pt *ProcessTracker) GetRecentErrors(limit int) []string {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	if len(pt.errors) <= limit {
		return pt.errors
	}
	return pt.errors[len(pt.errors)-limit:]
}

// printing the progress
func (pt *ProcessTracker) PrintProgress() {
	metrics := pt.GetMetrics()
	fmt.Printf("\r[%s] Progress: %.1f%% (%d/%d rows, %d/%d tables) | Speed: %.0f rows/sec | ETA: %v",
		time.Now().Format("22:29:56"),
		metrics.ProgressPercent,
		metrics.ProcessedRows,
		metrics.TotalRows,
		metrics.ProcessedTables,
		metrics.TotalTables,
		metrics.RowsPerSecond,
		formatDuration(metrics.EstimatedTimeLeft),
	)

	if metrics.CurrentTable != "" {
		fmt.Printf("| Current: %s", metrics.CurrentTable)
	}
}

// formats the duration in a human readable way
func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh%dm%ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	} else {
		return fmt.Sprintf("%ds", seconds)
	}
}

// starting goroutine for printing progress updates
func (pt *ProcessTracker) StartProgressMonitor(interval time.Duration) chan struct{} {
	stopChan := make(chan struct{})

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				pt.PrintProgress()
			case <-stopChan:
				pt.PrintProgress() //final update
				fmt.Println()      //new line after printing progress
				return
			}
		}
	}()
	return stopChan
}

// printing final progress summary
func (pt *ProcessTracker) PrintFinalSummary() {
	metrics := pt.GetMetrics()

	fmt.Println("\n=====Migration Summary====")
	fmt.Printf("Total Duration %v\n", formatDuration(metrics.ElapsedTime))
	fmt.Printf("Rows Processed: %d / %d (%.1f%%)\n", metrics.ProcessedRows, metrics.TotalRows, metrics.ProgressPercent)
	fmt.Printf("Tables Processed: %d / %d\n", metrics.ProcessedTables, metrics.TotalTables)
	fmt.Printf("Average Speed: %.0f rows/sec (%.0f rows/min)\n", metrics.RowsPerSecond, metrics.RowsPerSecond*60)
	fmt.Printf("Tables per Minute: %.1f\n", metrics.TablesPerMinute)

	if metrics.ErrorCount > 0 {
		fmt.Printf("Errrors Encountered: %d\n", metrics.ErrorCount)
		fmt.Println("\n Recent Errors:")
		for _, err := range pt.GetRecentErrors(5) {
			fmt.Printf(" -%s\n", err)
		}
	}
	fmt.Printf("=============")
}

// Health information
type HealthCheck struct {
	IsHealthy    bool          `json:"is_healthy"`
	LastActivity time.Time     `json:"last_activity"`
	Uptime       time.Duration `json:"uptime"`
	MemoryUsage  string        `json:"memory_usage"`
	Status       string        `json:"status"`
}

func (pt *ProcessTracker) GetHealthCheck() HealthCheck {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	timeSinceLastUpdate := time.Since(pt.lastUpdate)
	isHealthy := timeSinceLastUpdate < 30*time.Second //healthy if updated with 30secs

	var status string
	if pt.processedTables == pt.totalTables && atomic.LoadInt64(&pt.processedRows) == pt.totalRows {
		status = "completed"
	} else if isHealthy {
		status = "running"
	} else {
		status = "stalled"
	}

	return HealthCheck{
		IsHealthy:    isHealthy,
		LastActivity: pt.lastUpdate,
		Uptime:       time.Since(pt.startTime),
		Status:       status,
	}
}

// tracking within batches for better individual monitoring
type BatchTracker struct {
	pt             *ProcessTracker
	batchSize      int
	currentBatch   int
	batchStartTime time.Time
}

// creating a tracker for batch level operations
func (pt *ProcessTracker) NewBatchTracker(batchSize int) *BatchTracker {
	return &BatchTracker{
		pt:             pt,
		batchSize:      batchSize,
		batchStartTime: time.Now(),
	}
}

// begining of the new batch
func (bt *BatchTracker) StartBatch(batchNumber int) {
	bt.currentBatch = batchNumber
	bt.batchStartTime = time.Now()
}

// marking the batch as completed
func (bt *BatchTracker) CompleteBatch(rowsInBatch int64) {
	bt.pt.UpdateProgress(rowsInBatch)

	batchDuration := time.Since(bt.batchStartTime)
	if batchDuration > 0 {
		batchSpeed := float64(rowsInBatch) / batchDuration.Seconds() //since we need rows/sec
		fmt.Printf("\n Batch %d completed: %d rows in %v (%.0f rows/sec)", bt.currentBatch, rowsInBatch, formatDuration(batchDuration), batchSpeed)
	}
}

// structured logging the migration
type MigrationLogger struct {
	logChan chan LogEntry
	//file    string //todo: writting to a file
}

type LogEntry struct {
	TimeStamp time.Time `json:"time_stamp"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Table     string    `json:"table,omitempty"`
	RowCount  int64     `json:"row_count,omitempty"`
	Error     string    `json:"error,omitempty"`
}

// creating a new migration logger
func NewMigrationLogger() *MigrationLogger {
	ml := &MigrationLogger{
		logChan: make(chan LogEntry, 100),
	}

	//starting log processor
	go ml.processLogs()

	return ml
}

// processing log entries (todo: writting to a file)
func (ml *MigrationLogger) processLogs() {
	for entry := range ml.logChan {
		//printing stdout with formatting
		switch entry.Level {
		case "ERROR":
			fmt.Printf("[%s]ERROR: %s", entry.TimeStamp.Format("21:55:15"), entry.Message)
			if entry.Error != "" {
				fmt.Printf(" -%s\n", entry.Error)
			}
			fmt.Println()
		case "INFO":
			fmt.Printf("[%s] INFO: %s", entry.TimeStamp.Format("21:55:15"), entry.Message)
			if entry.Table != "" {
				fmt.Printf("(Table: %s", entry.Table)
				if entry.RowCount > 0 {
					fmt.Printf(", Rows: %d", entry.RowCount)
				}
				fmt.Printf(")")
			}
			fmt.Println()
		}
	}
}

// logging an info message
func (ml *MigrationLogger) Info(message string) {
	ml.logChan <- LogEntry{
		TimeStamp: time.Now(),
		Level:     "INFO",
		Message:   message,
	}
}

// logging an error message
func (ml *MigrationLogger) Error(message, errorMsg string) {
	ml.logChan <- LogEntry{
		TimeStamp: time.Now(),
		Level:     "ERROR",
		Message:   message,
		Error:     errorMsg,
	}
}

// logging tab1e specfic progress
func (ml *MigrationLogger) TableProgress(table string, rowCount int64, message string) {
	ml.logChan <- LogEntry{
		TimeStamp: time.Now(),
		Level:     "INFO",
		Message:   message,
		Table:     table,
		RowCount:  rowCount,
	}
}

// closing the logger
func (ml *MigrationLogger) Close() {
	close(ml.logChan)
}
