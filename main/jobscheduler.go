package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Job struct {
	ID            int
	JobName       string
	JobExecutable string
	StartTime     time.Time
	Period        time.Duration
	Priority      int
	Data          map[string]interface{}
}

var jobMap map[int]*Job
var jobQueue = make(chan *Job)
var resultQueue chan *Job
var mutex sync.Mutex
var jobDataMutex sync.Mutex
var jobProcessingMap = map[int]bool{}
var jobProcessingMapMutex sync.Mutex

func InsertJob(newJob *Job) (*Job, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if _, exists := jobMap[newJob.ID]; exists {
		return nil, fmt.Errorf("Job with ID %d already exists", newJob.ID)
	}

	jobMap[newJob.ID] = newJob

	return newJob, nil
}

func executeJob(job *Job) (bool, string) {
	// Replace this with your actual job execution logic.
	fmt.Printf("Executing Job: %s (ID: %d)\n", job.JobName, job.ID)
	return true, "" // Simulating success for simplicity.
}

func main() {
	jobMap = make(map[int]*Job)           // Map of Job ID to Job
	jobQueue = make(chan *Job, 100)       // Queue of Jobs to be executed
	resultQueue = make(chan *Job, 100)    // Queue of Jobs with results
	jobProcessingMap = make(map[int]bool) // Map to track processing jobs

	var wg sync.WaitGroup

	numWorkers := 8

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, &wg) // Start workers
	}

	go handleResults() // Handle results in a separate goroutine

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM) // Listen for SIGINT and SIGTERM

	job1 := &Job{
		ID:            1,
		JobName:       "Job 1",
		JobExecutable: "Executable 1",
		StartTime:     time.Now().Add(time.Second * 5), // Start after 5 seconds
		Period:        0,                               // Repeat every 10 seconds
		Priority:      1,
		Data:          map[string]interface{}{},
	}

	InsertJob(job1)

	for {
		select {
		case <-sig:
			close(jobQueue)
			wg.Wait()
			close(resultQueue)
			return

		default:
			now := time.Now()
			mutex.Lock()
			for _, job := range jobMap {
				if job.StartTime.Before(now) {
					// Check if the job is not already being processed
					jobProcessingMapMutex.Lock()
					defer jobProcessingMapMutex.Unlock()
					if !jobProcessingMap[job.ID] {
						jobQueue <- job
						// Mark the job as being processed
						jobProcessingMap[job.ID] = true
					}
				}
			}
			mutex.Unlock()
		}
	}

}

func handleResults() {
	fmt.Println("Handling results...")
	file, err := os.Create("results.csv")
	if err != nil {
		fmt.Println("Error creating CSV file:", err)
		return
	}
	writer := csv.NewWriter(file)

	for job := range resultQueue {
		var record []string
		jobDataMutex.Lock()
		if job.Data["Status"] == "COMPLETED" {
			record = []string{fmt.Sprintf("%d", job.ID), job.JobName, "COMPLETED"}
		} else if job.Data["Status"] == "FAILED" {
			record = []string{fmt.Sprintf("%d", job.ID), job.JobName, "FAILED", job.Data["Error"].(string)}
		}
		jobDataMutex.Unlock()
		if err := writer.Write(record); err != nil {
			fmt.Println("Error writing to CSV file:", err)
		}
		writer.Flush() // Ensure data is flushed to the file immediately
	}

	file.Close() // Close the file once all writing is done
}

func worker(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobQueue {
		success, errMsg := executeJob(job)

		if success {
			fmt.Printf("Worker %d completed Job: %s (ID: %d)\n", workerID, job.JobName, job.ID)
			// Remove job from map if it is not periodic
			if job.Period == 0 {
				mutex.Lock()
				delete(jobMap, job.ID)
				mutex.Unlock()
			} else {
				mutex.Lock()
				job.StartTime = time.Now().Add(job.Period)
				mutex.Unlock()
			}
		} else {
			fmt.Printf("Worker %d failed to execute Job: %s (ID: %d). Error: %s\n", workerID, job.JobName, job.ID, errMsg)
		}

		// Mark the job as not being processed
		jobProcessingMap[job.ID] = false

		// Update job data and add it to the resultQueue
		jobDataMutex.Lock()
		if success {
			job.Data["Status"] = "COMPLETED"
		} else {
			job.Data["Status"] = "FAILED"
			job.Data["Error"] = errMsg
		}
		jobDataMutex.Unlock()

		resultQueue <- job
	}
}
