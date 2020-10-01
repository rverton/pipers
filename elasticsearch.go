package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	Name    string                 `json:"name"`
	Data    map[string]interface{} `json:"data"`
	Created time.Time              `json:"created"`

	// back reference to original entry
	DocId    string `json:"doc_id"`
	DocIndex string `json:"doc_index"`
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

func (db *DB) retrieve(index string, filter map[string]string) ([]map[string]interface{}, error) {
	log.WithFields(
		log.Fields{"index": index, "filter": filter},
	).Debug("retrieving docs")

	searchResult, err := db.Find(index, filter)
	if err != nil {
		log.WithFields(log.Fields{"index": index}).Error(err)
		return nil, err
	}

	var results []map[string]interface{}

	for _, hit := range searchResult.Hits.Hits {
		var m map[string]interface{}
		err = json.Unmarshal(hit.Source, &m)
		if err != nil {
			return results, err
		}

		m = unflat(m)

		results = append(results, m)
	}

	log.WithFields(log.Fields{"index": index}).Debugf("query took %d milliseconds\n", searchResult.TookInMillis)

	return results, nil
}
