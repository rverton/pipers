package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	interval, _ := p.Interval()

	_, err = client.Enqueue(
		asynq.NewTask(TASK_PIPE, m),
		asynq.Unique(interval),
		asynq.Queue(p.Name),
		asynq.Timeout(timeout),
		asynq.MaxRetry(1),
	)
	return err
}

func parsePayload(t *asynq.Task) (pipe.Pipe, db.Data, error) {
	var data db.Data
	var p pipe.Pipe

	pipeData, err := t.Payload.GetString("pipe")
	if err != nil {
		log.Errorf("getting task payload failed: %v", err)
		return p, data, err
	}
	dataData, err := t.Payload.GetString("data")
	if err != nil {
		log.Errorf("getting task payload failed: %v", err)
		return p, data, err
	}

	if err := json.Unmarshal([]byte(pipeData), &p); err != nil {
		err := fmt.Errorf("unable to unmarshal pipe: %v", err)
		log.WithFields(log.Fields{"error": err}).Errorf("unable to unmarshal pipe")
		return p, data, err
	}

	if err := json.Unmarshal([]byte(dataData), &data); err != nil {
		err := fmt.Errorf("unable to unmarshal pipe: %v", err)
		log.Error(err)
		return p, data, err
	}

	return p, data, nil
}

// handler will be called when a job is received from the queue
func Handler(ctx context.Context, t *asynq.Task, ds db.DataService) error {
	p, data, err := parsePayload(t)
	if err != nil {
		log.Errorf("getting task payload failed: %v", err)
		return err
	}

	if data.Data == nil {
		data.Data = make(map[string]interface{})
	}

	// add task log
	if err := ds.AddTask(db.Task{
		Pipe:  p.Name,
		Ident: data.Id,
	}); err != nil {
		log.WithFields(log.Fields{"error": err}).Errorf("unable to add task")
	}

	// do not enqueue invalid assets
	if !pipe.IsValidHost(data.Asset) {
		log.WithFields(log.Fields{
			"pipe":  p.Name,
			"asset": data.Asset,
		}).Info("skipping asset pointing to blacklisted IP")
		return nil
	}

	if err := pipe.Process(ctx, p, data, ds); err != nil {
		return err
	}

	return nil
}

func ErrorHandler(ctx context.Context, task *asynq.Task, err error, saveFailed string) {
	retried, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)
	if retried >= maxRetry {
		// retry exhausted
		err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
	}

	// if the task timed out, skip error message
	if err == context.DeadlineExceeded {
		return
	}

	log.Errorf("handling queue task failed: %v\n", err)

	if task.Type == "pipe:handle" && saveFailed != "" {
		p, data, err2 := parsePayload(task)

		if err2 != nil {
			log.Errorf("saving failed task failed: %v", err2)
			return
		}

		combined := struct {
			Pipe  pipe.Pipe
			Data  db.Data
			Error string
		}{
			p, data, err.Error(),
		}

		encoded, err := json.MarshalIndent(combined, "", " ")
		if err != nil {
			log.Errorf("saving failed task failed: %v", err)
			return
		}

		tmpfile, err := ioutil.TempFile(saveFailed, "failed-")
		if err != nil {
			log.Fatal(err)
			return
		}

		defer tmpfile.Close()

		if _, err := tmpfile.Write(encoded); err != nil {
			log.Fatal(err)
			return
		}

		log.Info("saved failed task to", tmpfile.Name())
	}
}
