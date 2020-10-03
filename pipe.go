package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/Masterminds/sprig"
	"github.com/hibiken/asynq"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const INTERVAL_DEFAULT = "24h"

var SCHEDULER_SLEEP = time.Minute * 10

type Pipe struct {
	Name  string
	Input struct {
		Index  string
		Filter map[string]string
	}
	Command string `yaml:"cmd"`
	Output  struct {
		Index  string
		Ident  string
		Fields map[string]string
		Data   map[string]string
	}
	Interval string // time.Duration format
	Debug    bool
	Worker   int
}

func unflat(m map[string]interface{}) map[string]interface{} {
	unflatted := make(map[string]interface{})

	for k, v := range m {

		if strings.Contains(k, ".") {

			split := strings.SplitN(k, ".", 2)

			if _, ok := unflatted[split[0]]; !ok {
				unflatted[split[0]] = make(map[string]interface{})
			}

			tmp := unflatted[split[0]].(map[string]interface{})
			tmp[split[1]] = v

			unflatted[split[0]] = tmp

		} else {
			unflatted[k] = v
		}
	}

	return unflatted
}

func keys(m map[string]interface{}) []string {
	var s []string
	for k, _ := range m {
		s = append(s, k)
	}
	return s

}

func (p Pipe) interval() (time.Duration, error) {
	if p.Interval == "" {
		p.Interval = INTERVAL_DEFAULT
	}
	return time.ParseDuration(p.Interval)
}

func (p Pipe) validate() error {
	idx := p.Input.Index
	if idx != "assets" && idx != "services" && idx != "results" {
		return fmt.Errorf("invalid input: %v", p.Input)
	}

	if _, err := p.interval(); err != nil {
		return fmt.Errorf("invalid date interval: %w", err)
	}

	return nil
}

func (p Pipe) prepareCommand(input map[string]interface{}) (*exec.Cmd, error) {
	tplData := map[string]interface{}{
		"input": input,
	}

	s, err := Tpl(p.Command, tplData)
	if err != nil {
		return nil, fmt.Errorf("could not prepare command: %v", err)
	}

	return exec.Command("bash", "-c", s), nil
}

func Tpl(templateBody string, data map[string]interface{}) (string, error) {
	var b bytes.Buffer

	tmpl, err := template.New("result").Delims("${", "}").Funcs(sprig.TxtFuncMap()).Parse(templateBody)
	if err != nil {
		return "", fmt.Errorf("cant create template for: %v", err)
	}

	err = tmpl.Execute(&b, data)
	if err != nil {
		return "", fmt.Errorf("cant create output map: %v", err)
	}

	return b.String(), nil

}

func generateTemplateData(input map[string]interface{}, output []byte) map[string]interface{} {
	// try to parse as json, ignore if it fails
	var outputJson map[string]interface{}
	json.Unmarshal(output, &outputJson)

	return map[string]interface{}{
		"input":      input,
		"output":     string(output),
		"outputJson": outputJson,
	}
}

func (p Pipe) generateIdent(tplData map[string]interface{}) (string, error) {
	if p.Output.Ident == "" {
		return "", fmt.Errorf("ident field is empty")
	}

	return Tpl(p.Output.Ident, tplData)
}

func (p Pipe) outputMap(tplData map[string]interface{}) map[string]interface{} {
	doc := make(map[string]interface{})
	data := make(map[string]interface{})

	for name, val := range p.Output.Fields {
		s, err := Tpl(val, tplData)
		if err != nil {
			log.WithFields(log.Fields{"template": val}).Errorf("cant create template: %v", err)
			continue
		}

		doc[name] = s
	}

	for name, val := range p.Output.Data {
		s, err := Tpl(val, tplData)
		if err != nil {
			log.WithFields(log.Fields{"template": val}).Errorf("cant create template: %v", err)
			continue
		}

		data[name] = s
	}

	dataKey := fmt.Sprintf("data.%v", p.Name)

	doc[dataKey] = data

	return doc
}

func (p Pipe) run(db *DB, client *asynq.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		log.WithFields(log.Fields{
			"pipe": p.Name,
		}).Debugf("running")

		// retrieve (filtered) input
		data, err := db.retrieve(p.Input.Index, p.Input.Filter)
		if err != nil {
			log.Errorf("could not retrieve input: %v", err)
			break
		}

		for _, d := range data {

			inputId := d["_id"].(string)
			delete(d, "id")

			// ensure last task execution is long enough
			var found bool
			if found, err = db.lastTaskLongEnough(p, inputId); err != nil {
				log.WithFields(log.Fields{
					"pipe":  p.Name,
					"error": err,
				}).Error("get last task")
				continue
			}

			if found {
				log.WithFields(log.Fields{
					"pipe": p.Name,
					"id":   inputId,
				}).Debug("task too recent, skipping")
				continue
			}

			// enqueue task
			if err := enqueuePipe(p, d, client); err != nil {
				if errors.Is(err, asynq.ErrDuplicateTask) {
					log.WithFields(log.Fields{
						"pipe": p.Name,
					}).Info("enqueueing skipped, task already enqueued")
				} else {
					log.WithFields(log.Fields{
						"pipe": p.Name,
					}).Errorf("enqueueing failed: %v", err)
				}
			} else {
				log.WithFields(log.Fields{
					"pipe":     p.Name,
					"dataKeys": keys(d),
				}).Debug("enqueued")
			}

			// add task doc
			t := &Task{
				Pipe:    p.Name,
				Ident:   inputId,
				Created: time.Now(),
			}
			if _, err := db.Client.Index().Index("tasks").BodyJson(t).Do(db.ctx); err != nil {
				log.WithFields(log.Fields{
					"pipe": p.Name,
					"err":  err,
				}).Error("task save")
			}

		}

		duration, _ := p.interval()
		time.Sleep(SCHEDULER_SLEEP)
	}
}

func (p Pipe) handle(inputData map[string]interface{}, db *DB) error {
	cmd, err := p.prepareCommand(inputData)

	if err != nil {
		return fmt.Errorf("cant prepare pipe command: %v\n", err)
	}

	log.WithFields(log.Fields{
		"cmd": cmd.String(),
	}).Debug("executing")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("cant execute pipe command: %v\n", err)
	}

	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("cant execute pipe command: %v\n", err)
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		tplData := generateTemplateData(inputData, scanner.Bytes())

		result := p.outputMap(tplData)

		id, err := p.generateIdent(tplData)
		if err != nil {
			log.Error(err)
			continue
		}

		if id == "" {
			log.WithFields(log.Fields{
				"pipe":       p.Name,
				"identField": p.Output.Ident,
			}).Error("resulting ident is empty, skipping")
			continue
		}

		// only print output in debug mode
		if p.Debug {
			log.WithFields(log.Fields{
				"pipe":  p.Name,
				"ident": id,
			}).Infof("pipe debug: %+v", result)

			continue
		}

		if err := p.save(id, result, db); err != nil {
			log.WithFields(log.Fields{
				"pipe":  p.Name,
				"ident": id,
			}).Errorf("unable to save: %v", err)
		}
	}

	if err := cmd.Wait(); err != nil {
		log.WithFields(log.Fields{
			"pipe": p.Name,
		}).Errorf("pipe command failed: %v", err)
	}

	return nil
}

func (p Pipe) save(id string, result map[string]interface{}, db *DB) error {
	result["created"] = time.Now()

	upsert, err := db.Client.Update().
		Index(p.Output.Index).
		Id(id).
		Doc(result).
		DocAsUpsert(true).
		Do(db.ctx)
	if err != nil {
		return fmt.Errorf("cant upsert doc: %v", err)
	}

	if upsert.Result == "created" {
		if err := createAlert(db, p, result, id, "CREATED"); err != nil {
			log.WithFields(log.Fields{
				"pipe":  p.Name,
				"ident": id,
			}).Errorf("cant create alert: %v", err)
		}
	}

	return nil
}

func createAlert(db *DB, pipe Pipe, data map[string]interface{}, id, name string) error {
	log.WithFields(log.Fields{
		"pipe":  pipe.Name,
		"ident": id,
	}).Infof("created document")

	alert := Alert{
		Name:     name,
		DocIndex: pipe.Output.Index,
		DocId:    id,
		Data:     data,
		Created:  time.Now(),
	}

	_, err := db.Client.Index().Index("alerts").BodyJson(alert).Do(db.ctx)

	return err
}

func loadPipe(filename string) (Pipe, error) {
	var pipe Pipe
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		return pipe, err
	}

	err = yaml.Unmarshal(f, &pipe)
	if err != nil {
		return pipe, err
	}

	if err = pipe.validate(); err != nil {
		return pipe, err
	}

	return pipe, nil
}

func loadPipes(glob string) ([]Pipe, error) {
	var pipes []Pipe
	var err error

	files, err := filepath.Glob(glob)
	if err != nil {
		return pipes, err
	}

	for _, f := range files {
		p, err := loadPipe(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error loading %v: %v\n", f, err)
			continue
		}

		pipes = append(pipes, p)
	}

	return pipes, err
}
