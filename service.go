package models_utils

import (
	"os"
	"fmt"
	"log"
	"net/http"

	flags "github.com/jessevdk/go-flags"

	"github.com/rs/rest-layer/rest"
	"github.com/rs/rest-layer/resource"
)

type Options struct {
	Host        	string `long:"host" description:"the IP to listen on" default:"localhost" env:"HOST"`
	Port        	int    `long:"port" description:"the port to listen on for insecure connections, defaults to a random value" env:"PORT"`
	DatabaseSource string `long:"database-source" description:"" env:"DB_SOURCE"`
	KafkaSource    string `long:"kafka-source" description:"" env:"KAFKA_SOURCE"`
}

type CreateIndexFunc func(string, string) resource.Index
type SyncDataFunc func(string, string)

func parseOptionsOrExit() (opts Options) {
	if (os.Getenv("SERVICE_HOST") != "") {
		os.Setenv("HOST", os.Getenv("SERVICE_HOST"))
	}
	if (os.Getenv("SERVICE_PORT") != "") {
		os.Setenv("PORT", os.Getenv("SERVICE_PORT"))
	}

	var parser = flags.NewParser(&opts, flags.Default)

	if _, err := parser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}

	return opts
}

func Run(createIndex CreateIndexFunc, syncData SyncDataFunc) {
	opts := parseOptionsOrExit()

	index := createIndex(opts.DatabaseSource, "public")

	if syncData != nil {
		syncData(opts.DatabaseSource, opts.KafkaSource)
	}

	api, err := rest.NewHandler(index)
	if err != nil {
		log.Fatalf("Invalid API configuration: %s", err)
	}

	http.Handle("/", api)

	serverString := fmt.Sprintf("%s:%d", opts.Host, opts.Port)

	log.Printf("Serving API on %s", serverString)
	if err := http.ListenAndServe(serverString, nil); err != nil {
		log.Fatal(err)
	}
}