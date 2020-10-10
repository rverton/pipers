package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"

	sq "github.com/Masterminds/squirrel"
)

var db *pgxpool.Pool

const SQL_CREATE_DATA_TBL = `
CREATE TABLE IF NOT EXISTS %v (
	id text primary key,
	hostname text not null,
	target text not null,
	pipe text not null,
	data jsonb,
	created_at TIMESTAMP DEFAULT NOW()
);
`

const SQL_CREATE_ESSENTIALS = `
CREATE TABLE IF NOT EXISTS tasks (
	id serial primary key,
	pipe text not null,
	ident text not null,
	created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS tasks_pipe_idx ON tasks (pipe);
CREATE INDEX IF NOT EXISTS tasks_ident_idx ON tasks (ident);

CREATE TABLE IF NOT EXISTS alerts (
	id serial primary key,
	type text not null,
	pipe text not null,
	ident text not null,
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
	Created time.Time `json:"created_at"`
}

func NewDB(uri string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, fmt.Errorf("unable to parse DB URI")
	}

	return pgxpool.ConnectConfig(context.Background(), poolConfig)
}

func shouldRun(pipe, ident string, interval time.Duration) bool {
	var n int64

	err := db.QueryRow(
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

func tableExists(name string) bool {
	var n int64
	err := db.QueryRow(context.Background(), "select 1 from information_schema.tables where table_name=$1", name).Scan(&n)
	if err == pgx.ErrNoRows {
		return false
	} else if err != nil {
		return false
	}

	return true
}

func verifyDb(tables []string) error {
	_, err := db.Exec(context.Background(), SQL_CREATE_ESSENTIALS)
	if err != nil {
		return fmt.Errorf("unable to create essential tables: %v", err)
	}

	for _, name := range tables {

		// create table with SQL_CREATE_DATA_TBL
		_, err := db.Exec(context.Background(), fmt.Sprintf(SQL_CREATE_DATA_TBL, name))
		if err != nil {
			return fmt.Errorf("unable to create %v table: %v", name, err)
		}

	}

	return nil
}

// retrieve will return rows from the passed table and filtered by fields
// this method is prone to an sql injection in the table name, which is not
// important at this point because every pipe is a command execution by definition
func retrieve(table string, fields map[string]string) (pgx.Rows, error) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	query := psql.Select("id, hostname, target, data").From(table)
	for k, v := range fields {
		query = query.Where("(data ->> ?) = ?", k, v)
	}

	sql, args, err := query.ToSql()

	if err != nil {
		return nil, err
	}

	log.WithFields(log.Fields{"sql": sql, "args": args}).Debug("generated sql")

	return db.Query(context.Background(), sql, args...)
}

func retrieveByTarget(table string, fields map[string]string, target string) (pgx.Rows, error) {
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

	log.WithFields(log.Fields{"sql": sql, "args": args}).Debug("generated sql")

	return db.Query(context.Background(), sql, args...)
}

func retrieveTargets() ([]string, error) {

	var targets []string
	rows, err := db.Query(context.Background(), "SELECT DISTINCT target FROM assets")
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
