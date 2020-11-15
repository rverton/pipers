package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/rverton/pipers/db"
	"github.com/rverton/pipers/pipe"

	log "github.com/sirupsen/logrus"
)

const TASK_PIPE = "pipe:handle"
const TASK_LOCK = time.Hour * 2

func EnqueuePipe(p pipe.Pipe, data db.Data, client *asynq.Client) error {
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

	timeout, _ := p.Timeout()

	_, err = client.Enqueue(
		asynq.NewTask(TASK_PIPE, m),
		asynq.Unique(timeout),
		asynq.Queue(p.Name),
		asynq.Timeout(timeout),
		asynq.MaxRetry(3),
	)
	return err
}

// handler will be called when a job is received from the queue
func Handler(ctx context.Context, t *asynq.Task, ds db.DataService) error {
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

	var p pipe.Pipe
	if err := json.Unmarshal([]byte(pipeData), &p); err != nil {
		err := fmt.Errorf("unable to unmarshal pipe: %v", err)
		log.WithFields(log.Fields{"error": err}).Errorf("unable to unmarshal pipe")
		return err
	}

	var data db.Data
	if err := json.Unmarshal([]byte(dataData), &data); err != nil {
		err := fmt.Errorf("unable to unmarshal pipe: %v", err)
		log.Error(err)
		return err
	}

	// do not enqueue invalid assets
	if !pipe.IsValidHost(data.Asset) {
		log.WithFields(log.Fields{
			"pipe":     p.Name,
			"asset": data.Asset,
		}).Info("skipping asset pointing to blacklisted IP")
		return nil
	}

	if err := pipe.Process(ctx, p, data, ds); err != nil {
		log.WithFields(log.Fields{
			"pipe": p.Name,
		}).Errorf("pipe handle failed: %v", err)
		return err
	}

	return nil
}

func ErrorHandler(ctx context.Context, task *asynq.Task, err error) {
	retried, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)
	if retried >= maxRetry {
		// retry exhausted
		err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
	}
	log.Errorf("handling queue task failed: %v\n", err)
}
