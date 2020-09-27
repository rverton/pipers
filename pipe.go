package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sync"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/hibiken/asynq"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type Pipe struct {
	Name    string
	Input   string
	Filter  map[string]string
	Command string `yaml:"cmd"`
	Output  string
	Fields  map[string]string
	Data    map[string]string
	Ident   string
	Debug   bool
}

func (p Pipe) validate() error {
	if p.Input != "assets" {
		return fmt.Errorf("invalid input: %v", p.Input)
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
	if p.Ident == "" {
		return "", fmt.Errorf("ident field is empty")
	}

	return Tpl(p.Ident, tplData)
}

func (p Pipe) outputMap(tplData map[string]interface{}) map[string]interface{} {
	doc := make(map[string]interface{})
	data := make(map[string]interface{})

	for name, val := range p.Fields {
		s, err := Tpl(val, tplData)
		if err != nil {
			log.WithFields(log.Fields{"template": val}).Errorf("cant create template: %v", err)
			continue
		}

		doc[name] = s
	}

	for name, val := range p.Data {
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

	// retrieve (filtered) input
	data, err := db.retrieve(p.Input, p.Filter)
	if err != nil {
		log.Errorf("could not retrieve input: %v", err)
		return
	}

	// execute cmd for each input
	for _, d := range data {

		// enqueue task
		if err := p.enqueue(d, client); err != nil {
			log.WithFields(log.Fields{
				"pipe": p.Name,
			}).Errorf("enqueueing failed: %w", err)
		} else {
			log.WithFields(log.Fields{
				"pipe": p.Name,
				"data": d,
			}).Debug("enqueued")
		}

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
				"identField": p.Ident,
			}).Error("resulting ident is empty, skipping")
			continue
		}

		if p.Debug {
			log.WithFields(log.Fields{
				"pipe":  p.Name,
				"ident": id,
			}).Infof("pipe debug: %+v", result)

			continue
		}

		upsert, err := db.Client.Update().
			Index(p.Output).
			Id(id).
			Doc(result).
			DocAsUpsert(true).
			Do(db.ctx)
		if err != nil {
			return fmt.Errorf("cant upsert doc: %v", err)
		}

		if upsert.Result == "created" {
			log.WithFields(log.Fields{
				"pipe":  p.Name,
				"ident": id,
			}).Infof("new entry")
		}
	}

	cmd.Wait()

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

func loadPipes(folder string) ([]Pipe, error) {
	var pipes []Pipe
	var err error

	files, err := ioutil.ReadDir(folder)
	if err != nil {
		return pipes, err
	}

	for _, f := range files {
		fname := path.Join(folder, f.Name())
		p, err := loadPipe(fname)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error loading %v: %v\n", fname, err)
			continue
		}

		pipes = append(pipes, p)
	}

	return pipes, err
}
