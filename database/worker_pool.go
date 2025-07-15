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
func (wp *WorkerPool) work(id int) {
	defer wp.wg.Done()

	for job := range wp.jobs {
		fmt.Printf("Worker %d processing table: %s\n", id, job.TableName)
	}

	//fetching data from single table
	data, err := wp.fetchTableData(job.Client, job.TableName)

	result := TableResult{
		TableName: job.TableName,
		Data:      data,
		Error:     err,
	}
	wp.results <- result
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
