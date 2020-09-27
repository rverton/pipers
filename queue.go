package main

import (
	"github.com/hibiken/asynq"
)

const TASK_PIPE = "pipe:handle"

func (p Pipe) enqueue(data map[string]interface{}, client *asynq.Client) error {
	m := make(map[string]interface{})

	m["task"] = map[string]interface{}{
		"pipe": p,
		"data": data,
	}
	t := asynq.NewTask(TASK_PIPE, m)

	_, err := client.Enqueue(t)
	return err
}
