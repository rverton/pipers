package main

import (
	"flag"
	"sync"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"gopkg.in/sohlich/elogrus.v7"
)

const WORKER = 4

var db *DB

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

// worker will use asynq and redis to listen for pipe tasks to be handled
func worker() error {
	opts := asynq.RedisClientOpt{Addr: "localhost:6379"}
	srv := asynq.NewServer(opts, asynq.Config{
		Concurrency:  WORKER,
		ErrorHandler: asynq.ErrorHandlerFunc(queueErrorHandler),
		Logger:       log.StandardLogger(),
	})

	mux := asynq.NewServeMux()
	mux.HandleFunc(TASK_PIPE, handler)

	return srv.Run(mux)
}

// scheduler will load all pipes and add tasks to a queue
func scheduler() error {
	var err error

	pipes, err := loadPipes("./pipes/*.yml")
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
	var err error

	log.SetLevel(log.DebugLevel)

	workerMode := flag.Bool("worker", false, "start in worker mode")
	flag.Parse()

	// make DB available global
	db, err = NewDB()
	if err != nil {
		log.Fatal(err)
	}

	hook, err := elogrus.NewAsyncElasticHook(db.Client, "localhost", logrus.DebugLevel, "log")
	if err != nil {
		log.Panic(err)
	}
	log.AddHook(hook)

	if *workerMode {
		log.Info("starting worker")
		log.Error(worker())
		return
	}

	log.Info("starting scheduler")
	log.Error(scheduler())
}
