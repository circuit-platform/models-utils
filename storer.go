package models_utils

import (
	"log"
	"strings"
	"net/url"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/resource/testing/mem"
	mgo "gopkg.in/mgo.v2"
	mongo "github.com/rs/rest-layer-mongo"
	"github.com/apuigsech/rest-layer-sql"
)

func CreateStorerOrDie(databaseSource string, tableName string, config interface{}) (resource.Storer) {
	s, err := CreateStorer(databaseSource, tableName, config)
	if err != nil {
		log.Fatalf("Error creating storer: %\n", err)
	}
	return s
}

func CreateStorer(databaseSource string, tableName string, config interface{}) (resource.Storer, error) {
	u, err := url.Parse(databaseSource)
	if err != nil {
		return nil,err
	}

	var databaseName string
	pathElements := strings.Split(u.Path, "/")
	if (len(pathElements) > 1) {
		databaseName = pathElements[1]
	}

	switch u.Scheme {
	case "postgres":
		c := config.(*sqlStorage.Config)
		c.VerboseLevel = sqlStorage.DEBUG
		s, err := sqlStorage.NewHandler(u.Scheme, databaseSource, tableName, c)
		if err != nil {
			return nil, err
		}
		log.Printf("Connecting %s database on %s\n", u.Scheme, databaseSource)
		return s, nil
	case "mongodb":
		u.Path=""
		sess, err := mgo.Dial(u.String())
		if err != nil {
			return nil, err
		}

		log.Printf("Connecting %s database on %s\n", u.Scheme, databaseSource)
		return mongo.NewHandler(sess, databaseName, tableName), nil
	default:
		log.Printf("Connecting mem database")
		return  mem.NewHandler(), nil
	}
	return nil, nil
}