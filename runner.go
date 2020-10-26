package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/rverton/pipers/db"
	"github.com/rverton/pipers/pipe"
	"github.com/rverton/pipers/queue"
	log "github.com/sirupsen/logrus"
)

var SCHEDULER_SLEEP = time.Minute * 1

func runAsFile(p pipe.Pipe, client *asynq.Client, ds *db.DataService) error {
	targets, err := ds.RetrieveTargets()
	if err != nil {
		return fmt.Errorf("retrieving targets failed")
	}

	log.WithFields(log.Fields{
		"targets": len(targets),
		"pipe":    p.Name,
	}).Debug("retrieved targets for as_file")

	interval, _ := p.Interval()

	for _, target := range targets {

		if !ds.ShouldRun(p.Name, target, interval) {
			log.WithFields(log.Fields{
				"pipe":  p.Name,
				"ident": target,
			}).Info("task as_file too recent, skipping")
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
			err := rows.Scan(&data.Id, &data.Hostname, &data.Target, &data.Data)
			if err != nil {
				return fmt.Errorf("retrieving pipe input data failed: %v", err)
			}

			tpl, err := pipe.Tpl(p.Input.AsFile, map[string]interface{}{
				"input": pipe.MapInput(data),
			})
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Info("template for as_file failed")
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
			if errors.Is(err, asynq.ErrDuplicateTask) {
				log.WithFields(log.Fields{
					"pipe": p.Name,
				}).Info("enqueueing skipped, task already enqueued")
			} else {
				return fmt.Errorf("enqueuing failed: %v", err)
			}
		} else {
			log.WithFields(log.Fields{
				"pipe":    p.Name,
				"inputId": data.Id,
				"target":  target,
				"lines":   count,
			}).Info("enqueued as_file")

			ds.AddTask(db.Task{
				Pipe:  p.Name,
				Ident: target,
			})
		}

	}

	return nil
}

func runSingle(p pipe.Pipe, client *asynq.Client, ds *db.DataService) error {
	interval, _ := p.Interval()

	rows, err := ds.Retrieve(p.Input.Table, p.Input.Filter, interval)
	if err != nil {
		return fmt.Errorf("could not retrieve input: %v", err)
	}

	var tasks []db.Task

	count := 0
	for rows.Next() {
		count++

		data := db.Data{}
		err := rows.Scan(&data.Id, &data.Hostname, &data.Target, &data.Data)
		if err != nil {
			return fmt.Errorf("scanning pipe input failed: %v", err)
		}

		// enqueue task
		if err := queue.EnqueuePipe(p, data, client); err != nil {
			if errors.Is(err, asynq.ErrDuplicateTask) {
				log.WithFields(log.Fields{
					"pipe": p.Name,
				}).Debug("enqueueing skipped, task already enqueued")
			} else {
				return fmt.Errorf("enqueueing failed: %v", err)
			}
		} else {
			log.WithFields(log.Fields{
				"pipe":    p.Name,
				"inputId": data.Id,
			}).Info("enqueued")

			tasks = append(tasks, db.Task{
				Pipe:  p.Name,
				Ident: data.Id,
			})
		}
	}

	for _, t := range tasks {
		ds.AddTask(t)
	}

	log.WithFields(log.Fields{
		"pipe":    p.Name,
		"tasks":   len(tasks),
		"skipped": count - len(tasks),
	}).Info("scheduler run")

	return nil
}

// run will be executed for each pipe and is reponsible for
// scheduling tasks periodically
func run(p pipe.Pipe, client *asynq.Client, ds *db.DataService, wg *sync.WaitGroup) {
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
