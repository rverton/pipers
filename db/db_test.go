package db

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
)

const DB_URI = "dbname=pipers_test"

var TABLES = []string{"assets", "results"}

func testConnect() (*pgxpool.Pool, func()) {

	db, err := InitDb(DB_URI)
	if err != nil {
		panic(err)
	}

	for _, table := range append(TABLES, "alerts", "tasks") {
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

	tables := append(TABLES, "tasks", "alerts")

	for _, table := range tables {
		if !tableExists(db, table) {
			t.Errorf("table %v was not created", table)
		}
	}
}

func TestX(t *testing.T) {
	db, teardown := testConnect()
	defer teardown()
}
