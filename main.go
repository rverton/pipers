package main

import (
	"flag"
	"os"
	"sync"

	"github.com/hibiken/asynq"
	log "github.com/sirupsen/logrus"
)

const WORKER = 3

// tables returns a list of tables names
// from a list of pipes
func tables(pipes []Pipe) []string {
	tableMap := make(map[string]struct{})
	for _, p := range pipes {
		tableMap[p.Input.Table] = struct{}{}
		tableMap[p.Output.Table] = struct{}{}
	}

	var i []string
	for k := range tableMap {
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
	redisClient := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})

	var wg sync.WaitGroup
	for _, p := range pipes {
		wg.Add(1)

		log.WithFields(log.Fields{"pipe": p.Name}).Info("loaded pipe into scheduler")
		go p.run(redisClient, &wg)
	}

	wg.Wait()

	return nil
}

func main() {
	var err error
	var pipes []Pipe

	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	workerMode := flag.Bool("worker", false, "start in worker mode")
	single := flag.String("single", "", "path of a single pipe to execute")
	flag.Parse()

	// make DB available global
	db, err = NewDB(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

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

	if err := verifyDb(tables(pipes)); err != nil {
		log.WithFields(log.Fields{"tablesNames": tables(pipes), "error": err}).Fatal("db has not the correct schema")
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
