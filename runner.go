package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/rverton/pipers/db"
	"github.com/rverton/pipers/pipe"
	"github.com/rverton/pipers/queue"
	log "github.com/sirupsen/logrus"
)

var SCHEDULER_SLEEP = time.Minute * 1

func runAsFile(p pipe.Pipe, client *asynq.Client, ds db.DataService) error {
	logger := log.WithField("pipe", p.Name)

	targets, err := ds.RetrieveTargets()
	if err != nil {
		return fmt.Errorf("retrieving targets failed")
	}

	interval, _ := p.Interval()

	for _, target := range targets {

		if !ds.ShouldRun(p.Name, target, interval) {
			continue
		}

		rows, err := ds.RetrieveByTarget(p.Input.Table, p.Input.Filter, target)
		if err != nil {
			return fmt.Errorf("could not retrieve input: %v", err)
		}

		var data db.Data

		tmpInputFile, err := ioutil.TempFile(os.TempDir(), "pipers-tmp-")
		if err != nil {
			return fmt.Errorf("could not create tmp file: %v", err)
		}

		count := 0
		for rows.Next() {
			err := rows.Scan(&data.Id, &data.Asset, &data.Target, &data.Data)
			if err != nil {
				return fmt.Errorf("retrieving pipe input data failed: %v", err)
			}

			tpl, err := pipe.Tpl(p.Input.AsFile, map[string]interface{}{
				"input": pipe.MapInput(data),
			})
			if err != nil {
				logger.WithField("error", err).Error("template for as_file failed")
				continue
			}

			tmpInputFile.WriteString(tpl + "\n")
			count++
		}

		tmpInputFile.Close()

		if count <= 0 {
			continue
		}

		// take last data object and put input filename in
		newData := db.Data{
			Target: data.Target,
			Data: map[string]interface{}{
				"as_file": tmpInputFile.Name(),
			},
		}

		// enqueue task
		if err := queue.EnqueuePipe(p, newData, client); err != nil {
			if !errors.Is(err, asynq.ErrDuplicateTask) {
				return fmt.Errorf("enqueuing failed: %v", err)
			}
		} else {
			logger.WithFields(log.Fields{
				"inputId": data.Id,
				"target":  target,
				"lines":   count,
			}).Info("enqueued as_file")

			// add task now to prevent this resource intensive operation
			// to be run too often
			// TODO: move to retriever and add a redis check for this task
			ds.AddTask(db.Task{
				Pipe:  p.Name,
				Ident: target,
			})
		}

	}

	return nil
}

func runSingle(p pipe.Pipe, client *asynq.Client, ds db.DataService) error {
	interval, _ := p.Interval()
	count := 0
	countAdded := 0

	logger := log.WithFields(log.Fields{"pipe": p.Name})

	// retrieve data from file?
	if p.Input.File != "" {

		file, err := os.Open(p.Input.File)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()

			if !ds.ShouldRun(p.Name, line, interval) {
				continue
			}

			// enqueue task
			data := db.Data{
				Id:     line,
				Asset:  line,
				Target: filepath.Base(p.Input.File),
			}

			if err := queue.EnqueuePipe(p, data, client); err != nil {
				if !errors.Is(err, asynq.ErrDuplicateTask) {
					return fmt.Errorf("enqueueing failed: %v", err)
				}
			} else {
				logger.WithField("ident", data.Id).Info("enqueued")
				countAdded += 1
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

	} else {

		rows, err := ds.Retrieve(p.Input.Table, p.Name, p.Input.Filter, p.Input.Threshold, interval)
		if err != nil {
			return fmt.Errorf("could not retrieve input: %v", err)
		}

		for rows.Next() {
			count++

			data := db.Data{}
			err := rows.Scan(&data.Id, &data.Asset, &data.Target, &data.Data)
			if err != nil {
				return fmt.Errorf("scanning pipe input failed: %v", err)
			}

			// enqueue task
			if err := queue.EnqueuePipe(p, data, client); err != nil {
				if !errors.Is(err, asynq.ErrDuplicateTask) {
					return fmt.Errorf("enqueueing failed: %v", err)
				}
			} else {
				logger.WithField("ident", data.Id).Info("enqueued")
				countAdded += 1
			}
		}
	}

	logger.WithFields(log.Fields{
		"enqueued": countAdded,
		"skipped":  count - countAdded,
	}).Info("scheduler run")

	return nil
}

// run will be executed for each pipe and is reponsible for
// scheduling tasks periodically
func run(p pipe.Pipe, client *asynq.Client, ds db.DataService, wg *sync.WaitGroup) {
	defer wg.Done()

	for {

		// based on as_file, create a task with all results at once
		// or a single task for each returned record
		if p.Input.AsFile != "" {
			if err := runAsFile(p, client, ds); err != nil {
				log.WithFields(log.Fields{
					"pipe":  p.Name,
					"error": err,
				}).Error("running as file failed")
			}
		} else {
			if err := runSingle(p, client, ds); err != nil {
				log.WithFields(log.Fields{
					"pipe":  p.Name,
					"error": err,
				}).Error("run single failed")
			}

		}

		time.Sleep(SCHEDULER_SLEEP)
	}
}
