package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"

	log "github.com/sirupsen/logrus"
)

const TASK_PIPE = "pipe:handle"

func enqueuePipe(p Pipe, data map[string]interface{}, client *asynq.Client) error {
	m := make(map[string]interface{})

	pipeBytes, _ := json.Marshal(p)
	m["data"] = data
	m["pipe"] = string(pipeBytes)

	_, err := client.Enqueue(asynq.NewTask(TASK_PIPE, m))
	return err
}

func loggingMiddleware(h asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		log.Println("before")
		err := h.ProcessTask(ctx, t)
		if err != nil {
			log.Errorf("task execution failed: %v", err)
			return err
		}
		log.Println("after")
		//log.Printf("finished %q: Elapsed Time = %v", t.Type, time.Since(start))
		return nil
	})
}

func handler(ctx context.Context, t *asynq.Task) error {
	pipeData, err := t.Payload.GetString("pipe")
	if err != nil {
		log.Errorf("getting task payload failed: %w", err)
		return err
	}
	data, err := t.Payload.GetStringMap("data")
	if err != nil {
		log.Errorf("getting task payload failed: %w", err)
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
		}).Errorf("pipe handle failed: %w", err)
		return err
	}

	return nil
}
