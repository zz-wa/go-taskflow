package main

import (
	"errors"
	"fmt"
	"go-taskflow/model"
	"sync"
	"time"

	"github.com/google/uuid"
)

var mutex sync.RWMutex
var workerWg sync.WaitGroup
var jobWg sync.WaitGroup

func main() {
	model.JobStore, model.JobQueue = InitQueue()
	var ids []string
	for i := 0; i < 3; i++ {
		workerWg.Add(1)
		go StartWorker(i)
	}

	id := SubmitJob("getsuccess", "success")
	ids = append(ids, id)
	id = SubmitJob("1", "fail")
	ids = append(ids, id)
	id = SubmitJob("2", "flaky")
	ids = append(ids, id)

	jobWg.Wait()
	close(model.JobQueue)
	workerWg.Wait()

	for _, id := range ids {
		fmt.Println("job Status", GetJobStatus(id))
	}

}

func InitQueue() (map[string]*model.Job, chan *model.Job) {
	model.JobStore = make(map[string]*model.Job)
	model.JobQueue = make(chan *model.Job, 100)

	return model.JobStore, model.JobQueue
}

func SubmitJob(jobType, payload string) string {

	job := CreateJob(jobType, payload)
	jobWg.Add(1)
	model.JobQueue <- job

	return job.ID
}

func StartWorker(workerId int) {
	defer workerWg.Done()
	for job := range model.JobQueue {
		fmt.Printf("worker - %d begin job %s \n", workerId, job.ID)

		UpdateJobStatus(job.ID, model.Running)

		time.Sleep(1 * time.Second)

		err := ExecuteJob(job)
		if err != nil {
			HandleFail(job, err)
			continue
		}
		UpdateJobStatus(job.ID, model.Success)
		jobWg.Done()
		fmt.Printf("worker - %d success job %s \n", workerId, job.ID)
	}
}

func UpdateJobStatus(id string, status string) {
	mutex.Lock()
	defer mutex.Unlock()
	job := model.JobStore[id]
	job.Status = status
}

func GetJobStatus(id string) string {
	mutex.RLock()
	defer mutex.RUnlock()
	status := model.JobStore[id].Status
	return status
}

func CreateJob(jobType, payload string) *model.Job {
	id := uuid.New().String()
	job := &model.Job{

		Payload:    payload,
		JobType:    jobType,
		ID:         id,
		Status:     model.Pending,
		MaxRetries: 3,
		RetryTimes: 0,
	}
	mutex.Lock()
	model.JobStore[job.ID] = job
	mutex.Unlock()
	return job
}

func ExecuteJob(job *model.Job) error {

	switch job.Payload {
	case "success":
		return nil
	case "fail":
		return errors.New("execute failed")
	case "flaky":
		if job.RetryTimes < job.MaxRetries-1 {
			return errors.New("job retry failed")
		}

		return nil

	}

	return nil
}

func HandleFail(job *model.Job, err error) {
	retry := false

	mutex.Lock()
	job.RetryTimes++
	job.Error = err.Error()

	if job.RetryTimes < job.MaxRetries {
		job.Status = model.Pending
		retry = true
	} else {
		job.Status = model.Failed
	}
	mutex.Unlock()
	if retry {
		model.JobQueue <- job
	} else {
		jobWg.Done()
	}

}
