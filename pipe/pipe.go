package pipe

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig"

	"github.com/rverton/pipers/db"
	"github.com/rverton/pipers/notification"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

const INTERVAL_DEFAULT = "24h"
const TIMEOUT_DEFAULT = "1h"

type Pipe struct {
	Name  string
	Input struct {
		File      string
		Table     string
		Filter    map[string]string
		Threshold map[string]string
		AsFile    string `yaml:"as_file"`
	}
	Command string            `yaml:"cmd"`
	Filter  map[string]string // JS filter
	Output  struct {
		Table string
		Ident string
		Asset string
		Data  map[string]string
	}
	IntervalValue string `yaml:"interval"` // time.Duration format
	TimeoutValue  string `yaml:"timeout"`  // time.Duration format
	AlertMsgValue string `yaml:"alert_msg"`
	Debug         bool
	Worker        int
}

func (p Pipe) Interval() (time.Duration, error) {
	if p.IntervalValue == "" {
		p.IntervalValue = INTERVAL_DEFAULT
	}
	return time.ParseDuration(p.IntervalValue)
}

func (p Pipe) Timeout() (time.Duration, error) {
	if p.TimeoutValue == "" {
		p.TimeoutValue = TIMEOUT_DEFAULT
	}
	return time.ParseDuration(p.TimeoutValue)
}

func (p Pipe) Ident(tplData map[string]interface{}) (string, error) {
	if p.Output.Ident == "" {
		return "", fmt.Errorf("ident field is empty")
	}

	return Tpl(p.Output.Ident, tplData)
}

func (p Pipe) AlertMsg(tplData map[string]interface{}) (string, error) {
	if p.AlertMsgValue == "" {
		return "", nil
	}

	return Tpl(p.AlertMsgValue, tplData)
}

func (p Pipe) validate() error {
	if _, err := p.Interval(); err != nil {
		return fmt.Errorf("invalid date interval: %w", err)
	}

	if p.Command == "" {
		return fmt.Errorf("empty command")
	}

	return nil
}

func (p Pipe) prepareCommand(ctx context.Context, data db.Data) (*exec.Cmd, error) {
	tplData := map[string]interface{}{
		"input": MapInput(data),
	}

	s, err := Tpl(p.Command, tplData)
	if err != nil {
		return nil, fmt.Errorf("could not prepare command: %v", err)
	}

	return exec.CommandContext(ctx, "bash", "-c", s), nil
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

// generateTemplateData will bring the data in the correct
// structure for the yml fields
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

// outputMap will pass the template data to each individual output field
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

	s, err := Tpl(p.Output.Asset, tplData)
	if err != nil {
		log.WithFields(log.Fields{"template": p.Output.Asset}).Errorf("cant create template: %v", err)
	}

	data["asset"] = s

	return data
}

func (p Pipe) filter(vm *otto.Otto, output string) (bool, string, error) {
	err := vm.Set("output", output)
	if err != nil {
		return true, "", err
	}

	for name, f := range p.Filter {
		value, err := vm.Run(f)
		if err != nil {
			return true, "", err
		}

		result, err := value.ToBoolean()
		if err != nil {
			return true, "", err
		}

		if result {
			return true, name, nil
		}
	}

	return false, "", nil
}

func Process(ctx context.Context, p Pipe, data db.Data, ds db.DataService) error {
	start := time.Now()
	logger := log.WithField("pipe", p.Name)

	cmd, err := p.prepareCommand(ctx, data)

	if err != nil {
		return fmt.Errorf("cant prepare pipe command: %v\n", err)
	}

	logger.WithFields(log.Fields{
		"cmd":   cmd.String(),
		"asset": data.Asset,
	}).Debug("executing")

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("cant execute pipe command: %v\n", err)
	}

	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("cant execute pipe command: %v\n", err)
	}

	var notifyText string

	// initialize new JS engine for filtering
	vm := otto.New()

	// blocked domains
	blocked, err := ds.RetrieveBlocked()
	if err != nil {
		return fmt.Errorf("cant retrieve blocklist: %v\n", err)
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		b := scanner.Bytes()
		s := string(b)

		// skip if result is empty
		if strings.TrimSpace(s) == "" {
			continue
		}

		filter, filterName, err := p.filter(vm, s)
		if err != nil {
			logger.WithField("error", err).Error("filtering failed, skipping output")
			continue
		}

		if filter {
			logger.WithField("filter", filterName).Debug("filtered output")
			continue
		}

		tplData := generateTemplateData(data, b)
		output := p.outputMap(tplData)

		id, err := p.Ident(tplData)
		if err != nil {
			logger.WithField("error", err).Error(err)
			continue
		}

		if id == "" {
			logger.WithField("identField", p.Output.Ident).Error("resulting ident is empty, skipping")
			continue
		}

		// only print output in debug mode
		if p.Debug {
			log.WithField("ident", id).Infof("pipe debug: %+v", output)

			continue
		}

		asset := data.Asset
		if v, ok := output["asset"].(string); ok && v != "" {
			asset = v
		}

		if err := validateDomain(asset); err != nil {
			logger.WithFields(log.Fields{
				"ident": id,
				"asset": asset,
			}).Infof("invalid asset, skipping")

			continue
		}

		if isBlockedDomain(asset, blocked) {
			logger.WithFields(log.Fields{
				"ident": id,
				"asset": asset,
			}).Infof("blocked asset, skipping")
			continue
		}

		inserted, err := ds.Save(p.Output.Table, p.Name, id, data, output)
		if err != nil {
			logger.WithField("ident", id).Errorf("unable to save: %v", err)
			continue
		}

		if inserted {
			msg, err := p.AlertMsg(tplData)
			if err != nil {
				log.WithField("error", err).Errorf("generating alert failed")
			} else if msg != "" {
				notifyText += msg + "\n"
			}

			if err := ds.SaveAlert(p.Name, id, msg, "CREATED"); err != nil {
				log.WithField("ident", id).Errorf("cant create alert: %v", err)
			}
		}

	}

	if len(notifyText) > 0 {
		notifyText = fmt.Sprintf("*[%v]*\n%v", p.Name, notifyText)
		if err := notification.Notify(notifyText); err != nil {
			log.Errorf("slack webhook failed: %v", err)
		}
	}

	defer func() {
		// clean up if input was passed as a file
		if p.Input.AsFile != "" {
			if v, ok := data.Data["as_file"].(string); ok {
				if err := os.Remove(v); err != nil {
					logger.Errorf("could not remove as_file tmp file: %v", err)
				}
			} else {
				logger.Errorf("could not get as_file entry to remove temp file")
			}
		}

	}()

	if err := cmd.Wait(); err != nil {
		if err.Error() == "signal: killed" {
			logger.WithFields(log.Fields{
				"cmd": cmd.String(),
			}).Info("pipe timed out")
		} else {
			return err
		}
	}

	logger.WithFields(log.Fields{
		"asset":    data.Asset,
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

// mapInput enriches the data struct with a asset and a target
func MapInput(data db.Data) map[string]interface{} {
	input := data.Data
	input["asset"] = data.Asset
	input["target"] = data.Target
	return input
}
