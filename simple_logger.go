package main

import (
	"flag"
	"github.com/vsheffer/go-mongo-etl/gomongo"
	"log"
)

type SimpleLogger struct{}

func (sl *SimpleLogger) OnDelete(event *gooplog.OpLoggerEvent) {
	log.Printf("Deleted %s %+v", event.Id, event.Data)
}

func (sl *SimpleLogger) OnInsert(event *gooplog.OpLoggerEvent) {
	log.Printf("Inserted %s %+v", event.Id, event.Data)
}

func (sl *SimpleLogger) OnUpdate(event *gooplog.OpLoggerEvent) {
	log.Printf("Updated %s %+v", event.Id, event.Data)
}

func main() {
	url := flag.String("mongoUrl", "", "The mongo URL to use for connections.")
	label := "simpleLogger"
	filterRegex := "product.*"
	flag.Parse()

	tailer := gooplog.NewOpLogTailer(url, &filterRegex, &label, &SimpleLogger{})

	tailer.Start()
}
