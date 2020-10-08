package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
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
		Table  string
		Filter map[string]string
	}
	Command string `yaml:"cmd"`
	Output  struct {
		Table    string
		Ident    string
		Hostname string
		Data     map[string]string
	}
	Interval string // time.Duration format
	Debug    bool
	Worker   int
}

func keys(m map[string]interface{}) []string {
	var s []string
	for k := range m {
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
	idx := p.Input.Table
	if idx != "assets" && idx != "services" && idx != "results" {
		return fmt.Errorf("invalid input: %v", p.Input)
	}

	if _, err := p.interval(); err != nil {
		return fmt.Errorf("invalid date interval: %w", err)
	}

	return nil
}

// mapInput enriches the data struct with a hostname and a target
func mapInput(data Data) map[string]interface{} {
	input := data.Data
	input["hostname"] = data.Hostname
	input["target"] = data.Target
	return input
}

func (p Pipe) prepareCommand(data Data) (*exec.Cmd, error) {
	tplData := map[string]interface{}{
		"input": mapInput(data),
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

func generateTemplateData(data Data, output []byte) map[string]interface{} {
	// try to parse as json, ignore if it fails
	var outputJson map[string]interface{}
	json.Unmarshal(output, &outputJson)

	return map[string]interface{}{
		"input":      mapInput(data),
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
	data := make(map[string]interface{})

	for name, val := range p.Output.Data {
		s, err := Tpl(val, tplData)
		if err != nil {
			log.WithFields(log.Fields{"template": val}).Errorf("cant create template: %v", err)
			continue
		}

		data[name] = s
	}

	s, err := Tpl(p.Output.Hostname, tplData)
	if err != nil {
		log.WithFields(log.Fields{"template": p.Output.Hostname}).Errorf("cant create template: %v", err)
	}

	data["hostname"] = s

	return data
}

func (p Pipe) run(client *asynq.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		log.WithFields(log.Fields{
			"pipe": p.Name,
		}).Debugf("scheduling")

		// retrieve (filtered) input
		start := time.Now()
		rows, err := retrieve(p.Input.Table, p.Input.Filter)
		if err != nil {
			log.Errorf("could not retrieve input: %v", err)
			break
		}

		for rows.Next() {

			data := Data{}

			err := rows.Scan(&data.Id, &data.Hostname, &data.Target, &data.Data)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Info("retrieving pipe input data failed")
				continue
			}

			interval, _ := p.interval()

			if !shouldRun(p.Name, data.Id, interval) {
				log.WithFields(log.Fields{
					"pipe":  p.Name,
					"ident": data.Id,
				}).Info("task too recent, skipping")
				continue
			}

			// enqueue task
			if err := enqueuePipe(p, data, client); err != nil {
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
					"inputId":  data.Id,
					"dataKeys": keys(data.Data),
				}).Info("enqueued")
			}

			// add task
			t := Task{
				Pipe:  p.Name,
				Ident: data.Id,
			}
			if _, err := db.Exec(context.Background(), "INSERT INTO tasks (pipe, ident) VALUES ($1, $2)", t.Pipe, t.Ident); err != nil {
				log.WithFields(log.Fields{
					"task":  t,
					"error": err,
				}).Error("adding task failed")
			}
		}
		log.WithFields(log.Fields{"pipe": p.Name, "duration": time.Since(start)}).Debug("query executed")

		time.Sleep(SCHEDULER_SLEEP)
	}
}

func (p Pipe) handle(data Data) error {
	cmd, err := p.prepareCommand(data)

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
		tplData := generateTemplateData(data, scanner.Bytes())
		output := p.outputMap(tplData)

		id, err := p.generateIdent(tplData)
		if err != nil {
			log.WithFields(log.Fields{"error": err}).Error(err)
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
			}).Infof("pipe debug: %+v", output)

			continue
		}

		if err := p.save(id, data, output); err != nil {
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

func (p Pipe) save(id string, data Data, result map[string]interface{}) error {

	sql := fmt.Sprintf(`
		INSERT INTO %v (id, hostname, target, pipe, data) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING;
	`, p.Output.Table)

	// if a hostname is provided, this will overwrite it
	hostname := data.Hostname
	if v, ok := result["hostname"].(string); ok && v != "" {
		hostname = v
	}

	upsert, err := db.Exec(context.Background(), sql, id, hostname, data.Target, p.Name, result)
	if err != nil {
		return err
	}

	if upsert.RowsAffected() == 1 {
		log.WithFields(log.Fields{
			"pipe":  p.Name,
			"ident": id,
		}).Infof("created document")

		/*
			if err := createAlert(db, p, result, id, "CREATED"); err != nil {
				log.WithFields(log.Fields{
					"pipe":  p.Name,
					"ident": id,
				}).Errorf("cant create alert: %v", err)
			}
		*/
	}

	return nil
}

func createAlert(pipe Pipe, data *Data) error {

	// TODO: create alert

	/*
		log.WithFields(log.Fields{
			"pipe": pipe.Name,
			"id":   id,
		}).Debug("created alert")
	*/

	return nil
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
