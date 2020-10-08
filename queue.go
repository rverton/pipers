package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"

	log "github.com/sirupsen/logrus"
)

const TASK_PIPE = "pipe:handle"
const TASK_LOCK = time.Hour * 2
const TASK_TIMEOUT = TASK_LOCK

func enqueuePipe(p Pipe, data Data, client *asynq.Client) error {
	m := make(map[string]interface{})

	pipeBytes, err := json.Marshal(p)
	if err != nil {
		return err
	}
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	m["data"] = string(dataBytes)
	m["pipe"] = string(pipeBytes)

	_, err = client.Enqueue(
		asynq.NewTask(TASK_PIPE, m),
		asynq.Unique(TASK_LOCK), asynq.Queue(p.Name),
		asynq.Timeout(TASK_TIMEOUT),
	)
	return err
}

// handler will be called when a job is received
// this will include a pipe and data
func handler(ctx context.Context, t *asynq.Task) error {
	pipeData, err := t.Payload.GetString("pipe")
	if err != nil {
		log.Errorf("getting task payload failed: %v", err)
		return err
	}
	dataData, err := t.Payload.GetString("data")
	if err != nil {
		log.Errorf("getting task payload failed: %v", err)
		return err
	}

	var p Pipe
	if err := json.Unmarshal([]byte(pipeData), &p); err != nil {
		err := fmt.Errorf("unable to unmarshal pipe: %v", err)
		log.WithFields(log.Fields{"error": err}).Errorf("unable to unmarshal pipe")
		return err
	}

	var data Data
	if err := json.Unmarshal([]byte(dataData), &data); err != nil {
		err := fmt.Errorf("unable to unmarshal pipe: %v", err)
		log.Error(err)
		return err
	}

	if err := p.handle(data); err != nil {
		log.WithFields(log.Fields{
			"pipe": p.Name,
		}).Errorf("pipe handle failed: %v", err)
		return err
	}

	return nil
}

func queueErrorHandler(ctx context.Context, task *asynq.Task, err error) {
	retried, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)
	if retried >= maxRetry {
		// retry exhausted
		err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
	}
	log.Errorf("handling queue task failed: %v\n", err)
}
