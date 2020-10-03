package main

import (
	"flag"
	"sync"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"gopkg.in/sohlich/elogrus.v7"
)

const WORKER = 3

var db *DB

// indices returns a list of index names
// from a list of pipes
func indices(pipes []Pipe) []string {
	indexMap := make(map[string]struct{})
	for _, p := range pipes {
		indexMap[p.Input.Index] = struct{}{}
		indexMap[p.Output.Index] = struct{}{}
	}

	var i []string
	for k := range indexMap {
		i = append(i, k)
	}

	return i
}

// worker will use asynq and redis to listen for pipe tasks to be handled
// a server is started for each pipe with a specific queue
func startWorker(pipes []Pipe) {
	var wg sync.WaitGroup
	opts := asynq.RedisClientOpt{Addr: "localhost:6379"}

	for _, p := range pipes {
		if p.Worker <= 0 {
			p.Worker = 1
		}

		log.WithFields(log.Fields{"pipe": p.Name, "worker": p.Worker}).Debug("starting queue")

		srv := asynq.NewServer(opts, asynq.Config{
			Concurrency:  p.Worker,
			ErrorHandler: asynq.ErrorHandlerFunc(queueErrorHandler),
			Logger:       log.StandardLogger(),
			Queues: map[string]int{
				p.Name: 1,
			},
		})

		mux := asynq.NewServeMux()
		mux.HandleFunc(TASK_PIPE, handler)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			if err := srv.Run(mux); err != nil {
				log.Errorf("queue worker failed: %v", err)
			}
			wg.Done()
		}(&wg)
	}

	wg.Wait()
}

// scheduler will load all pipes and add tasks to a queue
func scheduler(pipes []Pipe) error {
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
	var pipes []Pipe

	log.SetLevel(log.DebugLevel)

	workerMode := flag.Bool("worker", false, "start in worker mode")
	single := flag.String("single", "", "path of a single pipe to execute")
	flag.Parse()

	// make DB available global
	db, err = NewDB()
	if err != nil {
		log.Fatal(err)
	}

	hook, err := elogrus.NewBulkProcessorElasticHook(db.Client, "localhost", logrus.DebugLevel, "log")
	if err != nil {
		log.Panic(err)
	}
	log.AddHook(hook)

	if *single == "" {
		pipes, err = loadPipes("./pipes/*.yml")
		if err != nil {
			log.Panic(err)
		}
	} else {
		pipe, err := loadPipe(*single)
		if err != nil {
			log.Panic(err)
		}

		pipes = append(pipes, pipe)
	}

	if *workerMode {
		log.Info("starting worker")
		startWorker(pipes)
		return
	}

	log.Info("starting scheduler")
	if err := scheduler(pipes); err != nil {
		log.Error(err)
	}
}
