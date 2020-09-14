package main

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

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

	if err := db.Setup(); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for _, p := range pipes {
		wg.Add(1)
		log.Infof("[%v] starting", p.Name)
		go p.run(db, &wg)
	}

	wg.Wait()
}
