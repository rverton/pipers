package db

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

const DB_URI = "host=localhost user=postgres password=postgres dbname=pipers_test"

var TABLES = []string{"domains", "services"}

func testConnect() (*pgxpool.Pool, func()) {

	db, err := InitDb(DB_URI)
	if err != nil {
		panic(err)
	}

	for _, table := range append(TABLES, "pipers_alerts", "pipers_tasks") {
		_, err = db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %v", table))
		if err != nil {
			panic(err)
		}
	}

	if err := SetupDb(db, TABLES); err != nil {
		panic(err)
	}

	return db, func() {
		db.Close()
	}

}

func TestSchemaCreation(t *testing.T) {
	// make DB available global
	db, err := InitDb(DB_URI)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := SetupDb(db, TABLES); err != nil {
		t.Errorf("error creating schema: %v", err)
	}

	tables := append(TABLES, "pipers_tasks", "pipers_alerts")

	for _, table := range tables {
		if !tableExists(db, table) {
			t.Errorf("table %v was not created", table)
		}
	}
}
func TestShouldRun(t *testing.T) {
	db, _ := testConnect()
	// defer teardown()

	ds := &PostgresService{DB: db}

	host := "robinverton.de"
	ident := "robinverton.de"
	target := "rv"
	filter := make(map[string]string)
	threshold := make(map[string]string)

	_, err := db.Exec(context.Background(), "INSERT INTO domains (id, asset, target, pipe) VALUES ($1, $2, $3, 'manual')", host, ident, target)
	if err != nil {
		t.Fatal(err)
	}

	// excluded asset
	_, err = db.Exec(context.Background(), "INSERT INTO domains (id, asset, target, pipe, exclude) VALUES ($1, $2, $3, 'manual', true)", host+"2", ident+"2", target)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("retrieves asset", func(t *testing.T) {
		rows, err := ds.Retrieve("domains", "http_detect", filter, threshold, time.Second*0)
		if err != nil {
			t.Error(err)
		}

		count := 0
		for rows.Next() {
			count++
		}

		if count != 1 {
			t.Errorf("want = 1, got = %v", count)
		}
	})

	t.Run("retrieves asset with interval", func(t *testing.T) {
		rows, err := ds.Retrieve("domains", "http_detect", filter, threshold, time.Hour*48)
		if err != nil {
			t.Error(err)
		}

		count := 0
		for rows.Next() {
			count++
		}

		if count != 1 {
			t.Errorf("want = 3, got = %v", count)
		}
	})

	t.Run("should not retrieve asset with task in the past", func(t *testing.T) {

		if err := ds.AddTask(Task{
			Pipe:  "http_detect",
			Ident: ident,
		}); err != nil {
			t.Error(err)
		}
		defer func() {
			db.Exec(context.Background(), "DELETE FROM pipers_tasks")
		}()

		rows, err := ds.Retrieve("domains", "http_detect", filter, threshold, time.Minute*1)
		if err != nil {
			t.Error(err)
		}

		got := testCountRows(rows)
		want := 0

		if got != want {
			t.Errorf("want = %v, got = %v", want, got)
		}
	})

	t.Run("should retrieve asset with differnt task in the past", func(t *testing.T) {

		if err := ds.AddTask(Task{
			Pipe:  "http_foobar",
			Ident: ident,
		}); err != nil {
			t.Error(err)
		}
		defer func() {
			db.Exec(context.Background(), "DELETE FROM pipers_tasks")
		}()

		rows, err := ds.Retrieve("domains", "http_detect", filter, threshold, time.Minute*1)
		if err != nil {
			t.Error(err)
		}

		got := testCountRows(rows)
		want := 1

		if got != want {
			t.Errorf("want = %v, got = %v", want, got)
		}
	})

	t.Run("should retrieve asset with differnt ident in the past", func(t *testing.T) {

		if err := ds.AddTask(Task{
			Pipe:  "http_detect",
			Ident: ident + "a",
		}); err != nil {
			t.Error(err)
		}
		defer func() {
			db.Exec(context.Background(), "DELETE FROM pipers_tasks")
		}()

		rows, err := ds.Retrieve("domains", "http_detect", filter, threshold, time.Minute*1)
		if err != nil {
			t.Error(err)
		}

		got := testCountRows(rows)
		want := 1

		if got != want {
			t.Errorf("want = %v, got = %v", want, got)
		}
	})
}

func TestShouldRunInputFilter(t *testing.T) {
	db, _ := testConnect()
	// defer teardown()

	ds := &PostgresService{DB: db}

	host := "robinverton.de"
	ident := "robinverton.de"
	target := "rv"
	filter := make(map[string]string)
	threshold := make(map[string]string)

	// asset with score of 100 and scope true
	_, err := db.Exec(context.Background(), "INSERT INTO domains (id, asset, target, pipe, data) VALUES ($1, $2, $3, 'manual', $4)", host+"3", ident+"3", target, `{"score":100, "scope":true}`)
	if err != nil {
		t.Fatal(err)
	}

	// asset with score of 200
	_, err = db.Exec(context.Background(), "INSERT INTO domains (id, asset, target, pipe, data) VALUES ($1, $2, $3, 'manual', $4)", host+"4", ident+"4", target, `{"score":200}`)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("retrieves asset with filter", func(t *testing.T) {
		f := map[string]string{
			"scope": "true",
		}
		rows, err := ds.Retrieve("domains", "http_detect", f, threshold, time.Second*0)
		if err != nil {
			t.Error(err)
		}

		count := 0
		for rows.Next() {
			count++
		}

		if count != 1 {
			t.Errorf("want = 1, got = %v", count)
		}
	})

	t.Run("retrieves asset with threshold", func(t *testing.T) {
		thresh := map[string]string{
			"score": "101",
		}
		rows, err := ds.Retrieve("domains", "http_detect", filter, thresh, time.Second*0)
		if err != nil {
			t.Error(err)
		}

		count := 0
		for rows.Next() {
			count++
		}

		if count != 1 {
			t.Errorf("want = 1, got = %v", count)
		}
	})

}

func testCountRows(r pgx.Rows) int {
	count := 0
	for r.Next() {
		count++
	}
	return count
}
