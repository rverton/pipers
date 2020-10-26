package main

import (
	"context"
	"flag"
	"os"
	"sync"

	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
	"github.com/rverton/pipers/db"
	"github.com/rverton/pipers/pipe"
	"github.com/rverton/pipers/queue"
	log "github.com/sirupsen/logrus"
)

const WORKER = 3
const LOGFILE = "/tmp/pipers.log"

func main() {
	var err error
	var pipes []pipe.Pipe

	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	if err = godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	workerMode := flag.Bool("worker", false, "start in worker mode")
	single := flag.String("single", "", "path of a single pipe to execute")
	flag.Parse()

	if *single == "" {
		pipes, err = pipe.LoadMultiple("./resources/pipes/*.yml")
		if err != nil {
			log.Panic(err)
		}
	} else {
		pipe, err := pipe.Load(*single)
		if err != nil {
			log.Panic(err)
		}

		pipes = append(pipes, pipe)
	}

	// make DB available global
	dbconn, err := db.InitDb(os.Getenv("DATABASE_URL"), pipe.Tables(pipes))
	if err != nil {
		log.Fatal(err)
	}
	defer dbconn.Close()

	ds := &db.DataService{DB: dbconn}

	if *workerMode {
		log.Info("starting worker")
		startWorker(pipes, ds)
	} else {
		log.Info("starting scheduler")
		if err := scheduler(pipes, ds); err != nil {
			log.Error(err)
		}
	}

}

// worker will use asynq and redis to listen for pipe tasks to be handled
// a server is started for each pipe with a specific queue
func startWorker(pipes []pipe.Pipe, ds *db.DataService) {
	var wg sync.WaitGroup
	opts := asynq.RedisClientOpt{Addr: "localhost:6379"}

	for _, p := range pipes {
		if p.Worker <= 0 {
			p.Worker = 1
		}

		log.WithFields(log.Fields{"pipe": p.Name, "worker": p.Worker}).Debug("starting queue")

		srv := asynq.NewServer(opts, asynq.Config{
			Concurrency:  p.Worker,
			ErrorHandler: asynq.ErrorHandlerFunc(queue.ErrorHandler),
			Logger:       log.StandardLogger(),
			Queues: map[string]int{
				p.Name: 1,
			},
		})

		mux := asynq.NewServeMux()
		mux.HandleFunc(queue.TASK_PIPE, func(c context.Context, t *asynq.Task) error {
			return queue.Handler(c, t, ds)
		})

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
func scheduler(pipes []pipe.Pipe, ds *db.DataService) error {
	redisClient := asynq.NewClient(asynq.RedisClientOpt{Addr: "localhost:6379"})

	var wg sync.WaitGroup
	for _, p := range pipes {
		wg.Add(1)

		log.WithFields(log.Fields{"pipe": p.Name}).Info("loaded pipe into scheduler")
		go run(p, redisClient, ds, &wg)
	}

	wg.Wait()

	return nil
}

func keys(m map[string]interface{}) []string {
	var s []string
	for k := range m {
		s = append(s, k)
	}
	return s

}
