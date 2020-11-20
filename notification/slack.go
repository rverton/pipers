package notification

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"time"
)

var slackWebhook string

type slackRequestBody struct {
	Text string `json:"text"`
}

func init() {
	s := os.Getenv("SLACK_WEBHOOK")

	if s != "" {
		slackWebhook = s
	}
}

func Notify(msg string) error {

	if slackWebhook != "" {
		return slackNotification(msg)
	}
	return nil
}

func slackNotification(msg string) error {

	slackBody, _ := json.Marshal(slackRequestBody{Text: msg})
	req, err := http.NewRequest(http.MethodPost, slackWebhook, bytes.NewBuffer(slackBody))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	if buf.String() != "ok" {
		return errors.New("Non-ok response returned from Slack")
	}
	return nil
}
