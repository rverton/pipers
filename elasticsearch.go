package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/olivere/elastic/v7"
)

const (
	mappingTarget = `
	{
		"settings":{
			"number_of_shards": 1,
			"number_of_replicas": 1
		},
		"mappings":{
			"properties":{
				"target_name":{
					"type":"keyword"
				},
				"hostname":{
					"type":"keyword"
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
	Services   []Service `json:"services"`
}

type Service struct {
	Port   int
	Banner string
	Data   string
}

func (a Asset) Id() string {
	return fmt.Sprintf("%x", md5.Sum([]byte(a.Hostname)))
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

func (db *DB) Setup() error {

	if err := db.ensureIndex("assets", mappingTarget); err != nil {
		return err
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
	log.Debugf("[%v] retrieving, filters=%+v\n", index, filter)

	searchResult, err := db.Find(index, filter)
	if err != nil {
		log.Printf("[%v] error: %v", index, err)
		return nil, err
	}

	var results []map[string]interface{}

	for _, hit := range searchResult.Hits.Hits {
		var m map[string]interface{}
		err = json.Unmarshal(hit.Source, &m)
		if err != nil {
			return results, err
		}

		results = append(results, m)
	}

	log.Debugf("[%v] query took %d milliseconds\n", index, searchResult.TookInMillis)

	return results, nil
}
