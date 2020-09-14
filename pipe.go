package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sync"
	"text/template"

	"github.com/emersion/go-imap/client"
	"github.com/olivere/elastic"
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
	Update  bool
}

func (p Pipe) validate() error {
	if p.Input != "assets" {
		return fmt.Errorf("invalid input: %v", p.Input)
	}

	return nil
}

func (p Pipe) prepareCommand(input map[string]interface{}) (*exec.Cmd, error) {
	tmpl, err := template.New("cmd").Parse(p.Command)
	if err != nil {
		return nil, err
	}

	var b bytes.Buffer
	err = tmpl.Execute(&b, input)
	if err != nil {
		return nil, err
	}

	return exec.Command("bash", "-c", b.String()), nil
}

func (p Pipe) outputMap(input map[string]interface{}, output string) map[string]interface{} {
	tplData := map[string]interface{}{
		"input":  input,
		"output": output,
	}

	doc := make(map[string]interface{})

	for name, val := range p.Fields {
		tmpl, err := template.New("result").Parse(val)
		if err != nil {
			log.Errorf("cant create template for '%v': %v", val, err)
			continue
		}

		var b bytes.Buffer

		err = tmpl.Execute(&b, tplData)
		if err != nil {
			log.Errorf("cant create output map: %v", err)
			continue
		}

		doc[name] = b.String()
	}

	return doc
}

func (p Pipe) run(db *DB, wg *sync.WaitGroup) {
	defer wg.Done()

	// retrieve (filtered) input
	data, err := db.retrieve(p.Input, p.Filter)
	if err != nil {
		log.Errorf("could not retrieve input: %v", err)
		return
	}

	// execute cmd for each input
	for _, d := range data {
		// TODO: this should be handled in a queue
		if err := p.handle(d, db); err != nil {
			log.Errorf("error handling output: %v", err)
		}
	}
}

func (p Pipe) handle(inputData map[string]interface{}, db *DB) error {

	cmd, err := p.prepareCommand(inputData)

	log.Debugf("executing %v", cmd.String())

	if err != nil {
		return fmt.Errorf("error preparing pipe command: %v\n", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("error executing pipe command: %v\n", err)
	}

	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("error executing pipe command: %v\n", err)
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		result := p.outputMap(inputData, scanner.Text())

		fmt.Printf("result: %+v\n", result)

		if p.Update {

			update, err := client.Update().Index("assets").Id(result["_id"]).
				Script(elastic.NewScriptInline("ctx._source.retweets += params.num").Lang("painless").Param("num", 1)).
				Upsert(map[string]interface{}{"retweets": 0}).
				Do(ctx)
			if err != nil {
				// Handle error
				panic(err)
			}

		} else {
			put, err := db.Client.Index().
				Index("assets").
				//Type(p.Output).
				BodyJson(result).
				Do(db.ctx)
			if err != nil {
				return fmt.Errorf("cant index doc: %v", err)
			}

			log.Debugf("indexed %v", put.Id)
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
