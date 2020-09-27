package main

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

// indices returns a list of index names
// from a list of pipes
func indices(pipes []Pipe) []string {
	indexMap := make(map[string]struct{})
	for _, p := range pipes {
		indexMap[p.Input] = struct{}{}
		indexMap[p.Output] = struct{}{}
	}

	var i []string
	for k := range indexMap {
		i = append(i, k)
	}

	return i
}

func main() {
	log.SetLevel(log.DebugLevel)

	pipes, err := loadPipes("./pipes/")
	if err != nil {
		panic(err)
	}

	db, err := NewDB()
	if err != nil {
		panic(err)
	}

	if err := db.Setup(indices(pipes)); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for _, p := range pipes {
		wg.Add(1)

		log.WithFields(log.Fields{"pipe": p.Name, "interval": p.Interval}).Info("loading")
		go p.run(db, &wg)
	}

	wg.Wait()
}
