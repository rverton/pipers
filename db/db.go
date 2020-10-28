package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"

	sq "github.com/Masterminds/squirrel"
)

const SQL_CREATE_DATA_TBL = `
CREATE TABLE IF NOT EXISTS %v (
	id text primary key,
	hostname text not null,
	target text not null,
	pipe text not null,
	data jsonb,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS %v_hostname_idx ON %v (hostname);
CREATE INDEX IF NOT EXISTS %v_target_idx ON %v (target);
`

const SQL_CREATE_ESSENTIALS = `
CREATE TABLE IF NOT EXISTS tasks (
	id serial primary key,
	pipe text not null,
	ident text not null,
	note text,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tasks_pipe_idx ON tasks (pipe);
CREATE INDEX IF NOT EXISTS tasks_ident_idx ON tasks (ident);

CREATE TABLE IF NOT EXISTS alerts (
	id serial primary key,
	type text not null,
	pipe text not null,
	ident text not null,
	message text,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS alerts_pipe_idx ON alerts (pipe);
CREATE INDEX IF NOT EXISTS alerts_ident_idx ON alerts (ident);
`

type Data struct {
	Id       string                 `json:"id"`
	Hostname string                 `json:"hostname"`
	Target   string                 `json:"target"`
	Pipe     string                 `json:"pipe"`
	Data     map[string]interface{} `json:"data"` // JSONB
}

type Alert struct {
	Type  string
	Pipe  string
	Ident string
}

type Task struct {
	Pipe    string    `json:"pipe"`
	Ident   string    `json:"ident"`
	Note    string    `json:"note"`
	Created time.Time `json:"created_at"`
}

type DataService struct {
	DB *pgxpool.Pool
}

func InitDb(uri string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, fmt.Errorf("unable to parse DB URI")
	}

	db, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, err
	}

	return db, err
}

func (d *DataService) AddTask(t Task) {
	if _, err := d.DB.Exec(context.Background(), "INSERT INTO tasks (pipe, ident, note) VALUES ($1, $2, $3)", t.Pipe, t.Ident, t.Note); err != nil {
		log.WithFields(log.Fields{
			"task":  t,
			"error": err,
		}).Error("adding task failed")
	}
}

func (d *DataService) ShouldRun(pipe, ident string, interval time.Duration) bool {
	var n int64

	err := d.DB.QueryRow(
		context.Background(),
		"SELECT 1 from tasks WHERE pipe = $1 and ident = $2 and created_at > NOW() - $3::interval LIMIT 1",
		pipe,
		ident,
		interval.Truncate(time.Millisecond).String(),
	).Scan(&n)

	if err == pgx.ErrNoRows {
		return true
	} else if err != nil {
		log.Errorf("cant count recent tasks: %v ", err)
	}

	return false
}

func tableExists(db *pgxpool.Pool, name string) bool {
	var n int64
	err := db.QueryRow(context.Background(), "select 1 from information_schema.tables where table_name=$1", name).Scan(&n)
	if err == pgx.ErrNoRows {
		return false
	} else if err != nil {
		return false
	}

	return true
}

func SetupDb(db *pgxpool.Pool, tables []string) error {
	_, err := db.Exec(context.Background(), SQL_CREATE_ESSENTIALS)
	if err != nil {
		return fmt.Errorf("unable to create essential tables: %v", err)
	}

	for _, name := range tables {

		// create table with SQL_CREATE_DATA_TBL
		_, err := db.Exec(context.Background(), fmt.Sprintf(SQL_CREATE_DATA_TBL, name, name, name, name, name))
		if err != nil {
			return fmt.Errorf("unable to create %v table: %v", name, err)
		}

	}

	return nil
}

// retrieve will return rows from the passed table, filtered by fields
// and only where no tasks is found (or the task is older than the passed
// interval. note that this function is vulnerable to sqli, but because
// a pipe in itself executes user commands, it does not matter here.
func (d *DataService) Retrieve(table string, fields map[string]string, interval time.Duration) (pgx.Rows, error) {
	sql := fmt.Sprintf(`
		SELECT
			A.id, A.hostname, A.target, A.data
		FROM 
			%v A
			LEFT JOIN tasks T
			ON A.id = T.ident AND A.pipe = T.pipe AND T.created_at > NOW() - $1::interval
		WHERE T.ident IS NULL
	`, table)

	var filterQuery []string
	args := []interface{}{interval}

	// filter from pipe
	count := 1
	for k, v := range fields {
		filterQuery = append(
			filterQuery,
			fmt.Sprintf("(data ->> $%v) = $%v", count+1, count+2),
		)
		args = append(args, k)
		args = append(args, v)
		count++
	}

	if len(filterQuery) > 0 {
		sql = fmt.Sprintf("%v AND ", sql)
	}

	sql = fmt.Sprintf("%v%v", sql, strings.Join(filterQuery, " AND "))

	return d.DB.Query(context.Background(), sql, args...)
}

func (d *DataService) RetrieveByTarget(table string, fields map[string]string, target string) (pgx.Rows, error) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	query := psql.Select("id, hostname, target, data").From(table)
	query = query.Where("target = ?", target)

	for k, v := range fields {
		query = query.Where("(data ->> ?) = ?", k, v)
	}

	sql, args, err := query.ToSql()

	if err != nil {
		return nil, err
	}

	return d.DB.Query(context.Background(), sql, args...)
}

func (d *DataService) RetrieveTargets() ([]string, error) {

	var targets []string
	rows, err := d.DB.Query(context.Background(), "SELECT DISTINCT target FROM assets")
	if err != nil {
		return targets, err
	}

	for rows.Next() {
		var t string
		if err = rows.Scan(&t); err != nil {
			return targets, err
		}
		targets = append(targets, t)
	}

	return targets, err
}

func (d *DataService) Save(table, pipe, id string, data Data, result map[string]interface{}) (bool, error) {

	sql := fmt.Sprintf(`
		INSERT INTO %v (id, hostname, target, pipe, data) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING;
	`, table)

	// if a hostname is provided, use the provided one if valid
	hostname := data.Hostname
	if v, ok := result["hostname"].(string); ok && v != "" {
		hostname = v
	}

	delete(result, "hostname")

	upsert, err := d.DB.Exec(context.Background(), sql, id, hostname, data.Target, pipe, result)
	if err != nil {
		return false, err
	}

	if upsert.RowsAffected() == 1 {
		return true, nil
	}

	return false, nil
}

func (d *DataService) SaveAlert(pipe string, id, msg, alertType string) error {

	sql := `INSERT INTO alerts (type, pipe, ident, message) VALUES ($1, $2, $3, $4)`

	_, err := d.DB.Exec(context.Background(), sql, alertType, pipe, id, msg)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"pipe": pipe,
		"id":   id,
	}).Debug("created alert")

	return nil
}
