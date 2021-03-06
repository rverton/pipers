package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
	"github.com/rverton/pipers/db"
	"github.com/rverton/pipers/notification"
	"github.com/rverton/pipers/pipe"
	"github.com/rverton/pipers/queue"
	log "github.com/sirupsen/logrus"
)

const WORKER = 3
const LOGFILE = "/tmp/pipers.log"

func main() {
	var err error
	var pipes []pipe.Pipe
	var ds db.DataService

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
	blacklist := flag.String("blacklist", "./resources/ips-exclude.txt", "file of IPs to exclude")
	noDb := flag.Bool("noDb", false, "do not use a database, read from stdin and print results")
	stdin := flag.Bool("stdin", false, "read from stdin")
	saveFailed := flag.String("saveFailed", "", "folder where failed tasks should be saved")
	replay := flag.String("replay", "", "replay a failed task")
	flag.Parse()

	if os.Getenv("SLACK_WEBHOOK") != "" {
		notification.SlackWebhook = os.Getenv("SLACK_WEBHOOK")
	}

	if err := pipe.LoadBlacklist(*blacklist); err != nil {
		log.Fatalf("could not load IP blacklist: %v", err)
	}

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

	if *noDb {
		ds = &db.PrintService{}
	} else {
		dbconn, err := db.InitDb(os.Getenv("DATABASE_URL"))
		if err != nil {
			log.Fatal(err)
		}
		defer dbconn.Close()

		if err := db.SetupDb(dbconn, pipe.Tables(pipes)); err != nil {
			log.Fatal(err)
		}

		ds = &db.PostgresService{DB: dbconn}
	}

	ro := asynq.RedisClientOpt{Addr: "localhost:6379"}

	if os.Getenv("REDIS") != "" {
		ro = asynq.RedisClientOpt{Addr: os.Getenv("REDIS")}
	}

	switch {
	case *stdin:
		log.Info("reading data from stdin")
		if err := process(pipes, ds); err != nil {
			log.Error(err)
		}
	case *workerMode:
		log.Info("starting worker")
		startWorker(pipes, ro, ds, *saveFailed)
	case *replay != "":
		log.Info("replaying task")
		if err := replayTask(*replay, ds); err != nil {
			log.Error(err)
		}
	default:
		log.Info("starting scheduler")
		if err := scheduler(pipes, ro, ds); err != nil {
			log.Error(err)
		}
	}

}

func replayTask(filename string, ds db.DataService) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	var tf queue.TaskFailed

	if err := json.Unmarshal(data, &tf); err != nil {
		return err
	}

	if err := pipe.Process(context.Background(), tf.Pipe, tf.Data, ds); err != nil {
		return fmt.Errorf("task processing failed: %v", err)
	}

	return nil
}
func process(pipes []pipe.Pipe, ds db.DataService) error {

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		for _, p := range pipes {
			data := db.Data{
				Asset: scanner.Text(),
				Data:  make(map[string]interface{}),
			}

			if err := pipe.Process(context.Background(), p, data, ds); err != nil {
				log.WithFields(log.Fields{
					"pipe": p.Name,
				}).Errorf("pipe handle failed: %v", err)
				return err
			}
		}
	}

	return nil
}

// worker will use asynq and redis to listen for pipe tasks to be handled
// a server is started for each pipe with a specific queue
func startWorker(pipes []pipe.Pipe, ro asynq.RedisClientOpt, ds db.DataService, saveFailed string) {
	var wg sync.WaitGroup

	for _, p := range pipes {
		if p.Worker <= 0 {
			p.Worker = 1
		}

		log.WithFields(log.Fields{"pipe": p.Name, "worker": p.Worker}).Debug("starting queue")

		srv := asynq.NewServer(ro, asynq.Config{
			Concurrency: p.Worker,
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				queue.ErrorHandler(ctx, task, err, saveFailed)
			}),
			Logger: log.StandardLogger(),
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
func scheduler(pipes []pipe.Pipe, ro asynq.RedisClientOpt, ds db.DataService) error {
	redisClient := asynq.NewClient(ro)

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
