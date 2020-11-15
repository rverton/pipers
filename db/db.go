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

type Data struct {
	Id     string                 `json:"id"`
	Asset  string                 `json:"asset"`
	Target string                 `json:"target"`
	Pipe   string                 `json:"pipe"`
	Data   map[string]interface{} `json:"data"` // JSONB
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

type DataService interface {
	AddTask(t Task) error
	ShouldRun(pipe, ident string, interval time.Duration) bool
	Retrieve(table, pipeName string, fields map[string]string, interval time.Duration) (pgx.Rows, error)
	RetrieveTargets() ([]string, error)
	RetrieveByTarget(table string, fields map[string]string, target string) (pgx.Rows, error)
	Save(table, pipe, id string, data Data, result map[string]interface{}) (bool, error)
	SaveAlert(pipe string, id, msg, alertType string) error
}

type PostgresService struct {
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

func (d *PostgresService) AddTask(t Task) error {
	if _, err := d.DB.Exec(context.Background(), "INSERT INTO pipers_tasks (pipe, ident, note) VALUES ($1, $2, $3)", t.Pipe, t.Ident, t.Note); err != nil {
		log.WithFields(log.Fields{
			"task":  t,
			"error": err,
		}).Error("adding task failed")
		return err
	}

	return nil
}

func (d *PostgresService) ShouldRun(pipe, ident string, interval time.Duration) bool {
	var n int64

	err := d.DB.QueryRow(
		context.Background(),
		"SELECT 1 from pipers_tasks WHERE pipe = $1 and ident = $2 and created_at > NOW() - $3::interval LIMIT 1",
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
func (d *PostgresService) Retrieve(table string, pipeName string, fields map[string]string, interval time.Duration) (pgx.Rows, error) {
	sql := fmt.Sprintf(`
		SELECT
			A.id, A.asset, A.target, A.data
		FROM 
			%v A
			LEFT JOIN pipers_tasks T
			ON A.id = T.ident AND T.pipe = $1 AND T.created_at > NOW() - $2::interval
		WHERE T.ident IS NULL
	`, table)

	var filterQuery []string
	args := []interface{}{pipeName, interval}

	// filter from pipe
	count := 2
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

func (d *PostgresService) RetrieveByTarget(table string, fields map[string]string, target string) (pgx.Rows, error) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	query := psql.Select("id, asset, target, data").From(table)
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

func (d *PostgresService) RetrieveTargets() ([]string, error) {

	var targets []string
	rows, err := d.DB.Query(context.Background(), "SELECT DISTINCT target FROM domains")
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

func (d *PostgresService) Save(table, pipe, id string, data Data, result map[string]interface{}) (bool, error) {

	sql := fmt.Sprintf(`
		INSERT INTO %v (id, asset, target, pipe, data) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING;
	`, table)

	// if a asset is provided, use the provided one if valid
	asset := data.Asset
	if v, ok := result["asset"].(string); ok && v != "" {
		asset = v
	}

	delete(result, "asset")

	upsert, err := d.DB.Exec(context.Background(), sql, id, asset, data.Target, pipe, result)
	if err != nil {
		return false, err
	}

	if upsert.RowsAffected() == 1 {
		return true, nil
	}

	return false, nil
}

func (d *PostgresService) SaveAlert(pipe string, id, msg, alertType string) error {

	sql := `INSERT INTO pipers_alerts (type, pipe, ident, message) VALUES ($1, $2, $3, $4)`

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
