package database

import (
	"fmt"
	"sync"
)

// manages the concurrent operations
type WorkerPool struct {
	numWorkers int
	jobs       chan TableJob
	results    chan TableResult
	wg         sync.WaitGroup
}

// represents the job to process the job
type TableJob struct {
	TableName string
	Client    DatabaseClient
}

// represents the result of processing a table
type TableResult struct {
	TableName string
	Data      []map[string]interface{}
	Error     error
}

// creating a new workerpool
func NewWorkerPool(numWorkers int) *WorkerPool {
	return &WorkerPool{
		numWorkers: numWorkers,
		jobs:       make(chan TableJob, numWorkers*2),
		results:    make(chan TableResult, numWorkers*2),
	}
}

// initialing the workerpool
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// processing jobs from the jobs channel
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for job := range wp.jobs {
		fmt.Printf("Worker %d processing table: %s\n", id, job.TableName)

		//fetching data from single table
		data, err := wp.fetchTableData(job.Client, job.TableName)

		result := TableResult{
			TableName: job.TableName,
			Data:      data,
			Error:     err,
		}
		wp.results <- result
	}
}

// fetching data from single table implementation
func (wp *WorkerPool) fetchTableData(client DatabaseClient, tableName string) ([]map[string]interface{}, error) {
	//todo - fill in fetchdata method for a single table
	return client.FetchAllData([]string{tableName})
}

// adding a job to the workerpool
func (wp *WorkerPool) SubmitJob(job TableJob) {
	wp.jobs <- job
}

// closing workerpool
func (wp *WorkerPool) Close() {
	close(wp.jobs)
	wp.wg.Wait()
	close(wp.results)
}

// returning result to the channel
func (wp *WorkerPool) GetResults() <-chan TableResult {
	return wp.results
}

// proessing multiple tables concurrently
func ProcessTablesWithWorkerPool(client DatabaseClient, tables []string, numWorkers int) ([]map[string]interface{}, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables to process")
	}

	//creating worker pool
	wp := NewWorkerPool(numWorkers)
	wp.Start()

	//submitting jobs to the pool
	go func() {
		for _, table := range tables {
			job := TableJob{
				TableName: table,
				Client:    client,
			}
			wp.SubmitJob(job)
		}
		wp.Close()
	}()

	//collecting results
	var allResults []map[string]interface{}
	var errors []error

	for i := 0; i < len(tables); i++ {
		result := <-wp.GetResults()

		if result.Error != nil {
			errors = append(errors, fmt.Errorf("error processing table %s: %w", result.TableName, result.Error))
			continue
		}

		//Adding table info to each row
		for j := range result.Data {
			result.Data[j]["_source_table"] = result.TableName
		}

		allResults = append(allResults, result.Data...)
		fmt.Printf("Completed processing table %s:%d rows", result.TableName, len(result.Data))
	}

	//returing error if any table fails
	if len(errors) > 0 {
		return nil, fmt.Errorf("failed to process %d tables: %v", len(errors), errors[0])
	}
	return allResults, nil
}

// for batch processing of data
type BatchProcessor struct {
	batchSize int
}

// creating a new batch processor
func NewBatchProcessor(batchsize int) *BatchProcessor {
	return &BatchProcessor{batchSize: batchsize}
}

// processing data in batches
func (bp *BatchProcessor) ProcessInBatches(data []map[string]interface{}, processFunc func([]map[string]interface{})) error {
	if len(data) == 0 {
		return nil
	}

	for i := 0; i < len(data); i += bp.batchSize {
		end := i + bp.batchSize
		if end > len(data) {
			end = len(data)
		}
		batch := data[i:end]
		if err := processFunc(batch); err != nil {
			return fmt.Errorf("failed to process the batch %d-%d:%w", i, end, &err)
		}

		fmt.Printf("Processed batch %d-%d (%d rows)", i, end, len(batch))
	}
	return nil
}
