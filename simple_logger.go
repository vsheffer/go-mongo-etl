package main

import (
	"flag"
	"github.com/vsheffer/go-mongo-etl/gomongo"
	"gopkg.in/mgo.v2/bson"
	"log"
)

type SimpleLogger struct{}

func (sl *SimpleLogger) OnDelete(deleted bson.M) {
	log.Printf("Deleted %+v", deleted)
}

func (sl *SimpleLogger) OnInsert(inserted bson.M) {
	log.Printf("Inserted %+v", inserted)
}

func (sl *SimpleLogger) OnUpdate(updated bson.M) {
	log.Printf("Updated %+v", updated)
}

func main() {
	url := flag.String("mongoUrl", "", "The mongo URL to use for connections.")
	label := "simpleLogger"
	filterRegex := "product.*"
	flag.Parse()

	tailer := gooplog.NewOpLogTailer(url, &filterRegex, &label, &SimpleLogger{})

	tailer.Start()
}
