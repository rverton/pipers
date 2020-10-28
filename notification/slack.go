package notification

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

var SlackWebhook string

type slackRequestBody struct {
	Text string `json:"text"`
}

func SlackNotification(msg string) error {

	if SlackWebhook == "" {
		return nil
	}

	slackBody, _ := json.Marshal(slackRequestBody{Text: msg})
	req, err := http.NewRequest(http.MethodPost, SlackWebhook, bytes.NewBuffer(slackBody))
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
