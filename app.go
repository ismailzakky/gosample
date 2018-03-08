package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/tokopedia/gosample/hello"
	_ "github.com/satori/go.uuid"
	grace "gopkg.in/tokopedia/grace.v1"
	logging "gopkg.in/tokopedia/logging.v1"
	uuid2 "github.com/satori/go.uuid"
)

func main() {

	flag.Parse()
	logging.LogInit()

	debug := logging.Debug.Println

	debug("app started")

	uuid,_ := uuid2.NewV1()
	uuidString := uuid.String()
	hwm := hello.NewHelloWorldModule()
	hwm.SendMessage("Ismail Zakky", "Welcome Aboard ("+uuidString+")")
	http.HandleFunc("/select", hwm.GetMultiDataFromDatabase)
	http.HandleFunc("/change", hwm.ChangeWelcomeMessage)
	http.HandleFunc("/search", hwm.SearchDataFromDatabase)
	http.HandleFunc("/getTitle", hwm.GetTitle)
	go logging.StatsLog()

	log.Fatal(grace.Serve(":9000", nil))
}
