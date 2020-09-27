package main

import (
	"context"
	"flag"
	"fmt"
	"sync"

	"github.com/hibiken/asynq"
	log "github.com/sirupsen/logrus"
)

// indices returns a list of index names
// from a list of pipes
func indices(pipes []Pipe) []string {
	indexMap := make(map[string]struct{})
	for _, p := range pipes {
		indexMap[p.Input] = struct{}{}
		indexMap[p.Output] = struct{}{}
	}

	var i []string
	for k := range indexMap {
		i = append(i, k)
	}

	return i
}

func handler(ctx context.Context, t *asynq.Task) error {
	taskData, err := t.Payload.GetStringMap("task")
	if err != nil {
		return err
	}
	pipe := taskData["pipe"].(Pipe)
	data := taskData["data"]
	fmt.Printf("%v, %v", pipe.Name, data)

	return nil
}

func worker() error {
	opts := asynq.RedisClientOpt{Addr: "localhost:6379"}
	srv := asynq.NewServer(opts, asynq.Config{
		Concurrency: 10,
	})

	mux := asynq.NewServeMux()
	mux.HandleFunc(TASK_PIPE, handler)

	return srv.Run(mux)
}

func scheduler() error {
	pipes, err := loadPipes("./pipes/")
	if err != nil {
		return err
	}

	db, err := NewDB()
	if err != nil {
		return err
	}

	if err := db.Setup(indices(pipes)); err != nil {
		return err
	}

	redisClient := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})

	var wg sync.WaitGroup
	for _, p := range pipes {
		wg.Add(1)

		log.WithFields(log.Fields{"pipe": p.Name}).Info("starting scheduler")
		go p.run(db, redisClient, &wg)
	}

	wg.Wait()

	return nil
}

func main() {
	log.SetLevel(log.DebugLevel)

	workerMode := flag.Bool("worker", false, "start in worker mode")
	flag.Parse()

	if *workerMode {
		log.Info("starting worker")
		log.Error(worker())
	} else {
		log.Info("starting scheduler")
		log.Error(scheduler())
	}
}
