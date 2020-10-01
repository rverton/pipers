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
const TASK_LOCK = time.Hour * 24

func enqueuePipe(p Pipe, data map[string]interface{}, client *asynq.Client) error {
	m := make(map[string]interface{})

	pipeBytes, _ := json.Marshal(p)
	m["data"] = data
	m["pipe"] = string(pipeBytes)

	_, err := client.Enqueue(asynq.NewTask(TASK_PIPE, m), asynq.Unique(TASK_LOCK))
	return err
}

func handler(ctx context.Context, t *asynq.Task) error {
	pipeData, err := t.Payload.GetString("pipe")
	if err != nil {
		log.Errorf("getting task payload failed: %v", err)
		return err
	}
	data, err := t.Payload.GetStringMap("data")
	if err != nil {
		log.Errorf("getting task payload failed: %v", err)
		return err
	}

	var p Pipe
	if err := json.Unmarshal([]byte(pipeData), &p); err != nil {
		err := fmt.Errorf("unable to unmarshal pipe: %v", err)
		log.Error(err)
		return err
	}

	if err := p.handle(data, db); err != nil {
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
