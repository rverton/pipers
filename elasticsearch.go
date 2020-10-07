package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/olivere/elastic/v7"
)

const (
	mappingTarget = `
	{
		"mappings":{
			"properties":{
				"target_name":{
					"type":"keyword"
				},
				"hostname":{
					"type":"keyword"
				},
				"service":{
					"type":"keyword"
				},
				"ident":{
					"type":"keyword"
				},
				"alert_type":{
					"type":"keyword"
				},
				"doc_index":{
					"type":"keyword"
				},
				"doc_id":{
					"type":"keyword"
				},
				"created":{
					"type":"date"
				}
			}
		}
	}
	`
)

type DB struct {
	Client *elastic.Client
	ctx    context.Context
}

type Asset struct {
	TargetName string    `json:"target_name"`
	Hostname   string    `json:"hostname"`
	Created    time.Time `json:"created"`
}

type Alert struct {
	AlertType string                 `json:"alert_type"`
	Data      map[string]interface{} `json:"data"`

	// back reference to original entry
	DocId    string `json:"doc_id"`
	DocIndex string `json:"doc_index"`
}

type Task struct {
	Pipe  string `json:"pipe"`
	Ident string `json:"ident"`

	Created time.Time `json:"created"`
}

func NewDB() (*DB, error) {
	var err error
	db := &DB{
		ctx: context.Background(),
	}
	db.Client, err = elastic.NewClient(
		elastic.SetURL("http://localhost:9200"),
		elastic.SetSniff(false),
	)

	return db, err
}

func (db *DB) ensureIndex(name, mapping string) error {

	exists, err := db.Client.IndexExists(name).Do(db.ctx)
	if err != nil {
		return err
	}

	if !exists {
		log.Printf("index %v does not exist, creating", name)
		createIndex, err := db.Client.CreateIndex(name).BodyString(mapping).Do(db.ctx)
		if err != nil {
			return err
		}
		if !createIndex.Acknowledged {
			return fmt.Errorf("index creation not acknowledged")
		}
	}

	return nil
}

func (db *DB) Setup(indices []string) error {
	indices = append(indices, "alerts")
	indices = append(indices, "results")
	indices = append(indices, "tasks")

	for _, indexName := range indices {
		if err := db.ensureIndex(indexName, mappingTarget); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Find(index string, fields map[string]string) (*elastic.SearchResult, error) {

	query := elastic.NewBoolQuery()

	for k, v := range fields {
		query = query.Must(elastic.NewTermQuery(k, v))
	}

	searchResult, err := db.Client.Search().
		Index(index).
		Query(query).
		Do(db.ctx)
	if err != nil {
		return nil, err
	}

	return searchResult, nil

}

func dumpQuery(q interface{}) {
	b, _ := json.MarshalIndent(q, " ", " ")
	fmt.Println(string(b))
}

func (db *DB) lastTaskLongEnough(pipe Pipe, id string) (bool, error) {
	query := elastic.NewBoolQuery().
		Must(elastic.NewTermQuery("pipe", pipe.Name)).
		Must(elastic.NewTermQuery("ident", id)).
		Must(elastic.NewRangeQuery("created").Gte(fmt.Sprintf("now-%v", pipe.Interval)))

	searchResult, err := db.Client.Search().
		Index("tasks").
		Size(1).
		Query(query).
		Do(db.ctx)
	if err != nil {
		return false, err
	}

	return searchResult.TotalHits() > 0, nil
}

func (db *DB) retrieve(index string, filter map[string]string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	log.WithFields(
		log.Fields{"index": index, "filter": filter},
	).Debug("retrieving docs")

	query := elastic.NewBoolQuery()

	for k, v := range filter {
		query = query.Must(elastic.NewTermQuery(k, v))
	}

	begin := time.Now()
	count := 0

	svc := db.Client.Scroll(index).Query(query)
	for {
		res, err := svc.Do(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			return results, err
		}
		for _, hit := range res.Hits.Hits {
			count++

			var m map[string]interface{}
			err = json.Unmarshal(hit.Source, &m)
			if err != nil {
				return results, err
			}

			m["_id"] = hit.Id

			m = unflat(m)

			results = append(results, m)
		}
	}

	log.WithFields(log.Fields{"index": index, "duration": time.Since(begin), "results": count}).Debug("query executed", time.Since(begin))

	return results, nil
}
