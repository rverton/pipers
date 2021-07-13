package db

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
)

type PrintService struct{}

func (p *PrintService) AddTask(t Task) error { return nil }

func (p *PrintService) ShouldRun(pipe, ident string, interval time.Duration) bool {
	return true
}

func (p *PrintService) Retrieve(table, pipeName string, fields map[string]string, threshold map[string]string, interval time.Duration) (pgx.Rows, error) {
	return nil, nil
}

func (p *PrintService) RetrieveTargets() ([]string, error) {
	return []string{}, nil
}

func (p *PrintService) RetrieveBlocked() ([]string, error) {
	return []string{}, nil
}

func (p *PrintService) RetrieveByTarget(table string, fields map[string]string, target string) (pgx.Rows, error) {
	return nil, nil
}

func (p *PrintService) Save(table, pipe, id string, data Data, result map[string]interface{}) (bool, error) {
	fmt.Printf("table=%v, ident=%v, pipe=%v\ndata=%+v\nresult=%+v\n", table, id, pipe, data, result)
	return true, nil
}

func (p *PrintService) SaveAlert(pipe string, id, msg, alertType string) error {
	return nil
}
