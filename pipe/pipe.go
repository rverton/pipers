package pipe

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"text/template"
	"time"

	"github.com/Masterminds/sprig"
	"github.com/rverton/pipers/db"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const INTERVAL_DEFAULT = "24h"

type Pipe struct {
	Name  string
	Input struct {
		Table  string
		Filter map[string]string
		AsFile string `yaml:"as_file"`
	}
	Command string `yaml:"cmd"`
	Output  struct {
		Table    string
		Ident    string
		Hostname string
		Data     map[string]string
	}
	interval string `yaml:"interval"` // time.Duration format
	Debug    bool
	Worker   int
}

func (p Pipe) Interval() (time.Duration, error) {
	if p.interval == "" {
		p.interval = INTERVAL_DEFAULT
	}
	return time.ParseDuration(p.interval)
}

func (p Pipe) validate() error {
	if _, err := p.Interval(); err != nil {
		return fmt.Errorf("invalid date interval: %w", err)
	}

	return nil
}

func (p Pipe) prepareCommand(data db.Data) (*exec.Cmd, error) {
	tplData := map[string]interface{}{
		"input": MapInput(data),
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

func generateTemplateData(data db.Data, output []byte) map[string]interface{} {
	// try to parse as json, ignore if it fails
	var outputJson map[string]interface{}
	json.Unmarshal(output, &outputJson)

	return map[string]interface{}{
		"input":      MapInput(data),
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

func Process(p Pipe, data db.Data, ds *db.DataService) error {
	start := time.Now()

	cmd, err := p.prepareCommand(data)

	if err != nil {
		return fmt.Errorf("cant prepare pipe command: %v\n", err)
	}

	log.WithFields(log.Fields{
		"pipe":     p.Name,
		"cmd":      cmd.String(),
		"hostname": data.Hostname,
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

		if err := ds.Save(p.Output.Table, p.Name, id, data, output); err != nil {
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

	// clean up if input was passed as a file
	if p.Input.AsFile != "" {
		if v, ok := data.Data["as_file"].(string); ok {
			if err := os.Remove(v); err != nil {
				log.Errorf("could not remove as_file tmp file: %v", err)
			}
		} else {
			log.Errorf("could not get as_file entry to remove temp file")
		}
	}

	log.WithFields(log.Fields{
		"pipe":     p.Name,
		"hostname": data.Hostname,
		"duration": time.Since(start),
	}).Info("execution finished")

	return nil
}

func Load(filename string) (Pipe, error) {
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

func LoadMultiple(glob string) ([]Pipe, error) {
	var pipes []Pipe
	var err error

	files, err := filepath.Glob(glob)
	if err != nil {
		return pipes, err
	}

	for _, f := range files {
		p, err := Load(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error loading %v: %v\n", f, err)
			continue
		}

		pipes = append(pipes, p)
	}

	return pipes, err
}

// mapInput enriches the data struct with a hostname and a target
func MapInput(data db.Data) map[string]interface{} {
	input := data.Data
	input["hostname"] = data.Hostname
	input["target"] = data.Target
	return input
}
