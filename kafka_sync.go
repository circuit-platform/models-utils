package models_utils

import (
	"log"
	"context"
	"bytes"
	"time"
	"strings"
	"encoding/json"

	"github.com/rs/rest-layer/schema"
	"github.com/rs/rest-layer/resource"
	kafka "github.com/segmentio/kafka-go"
)

type KafkaMessage struct {

	Payload struct {
		Op string
		Before *map[string]interface{}
		After *map[string]interface{}
	}
}

func SyncIndex(readerConfig kafka.ReaderConfig, index resource.Index) {
	if c, ok := index.(resource.Compiler); ok {
		if err := c.Compile(); err != nil {
			log.Println("Error Compiling Index:", err)
		}
	}

	for _,r := range index.GetResources() {
		SyncResource(readerConfig, r)
	}
}

func SyncResource(_space string, _readerConfig kafka.ReaderConfig, _resource *resource.Resource) {
	readerConfig := _readerConfig
	readerConfig.Topic = _space + "." + strings.ReplaceAll(_resource.Path(), ".", "_")

	go func() {
		reader := kafka.NewReader(readerConfig)

		log.Printf("Consuming kafka topic: %s\n", readerConfig.Topic)

		for {
			m, err := reader.ReadMessage(context.Background())
			if err != nil {
        		log.Printf("Error reading Kafka message: %s", err)
        		continue		
			}

			var msg KafkaMessage
			d := json.NewDecoder(bytes.NewBuffer(m.Value))
			d.UseNumber()
			d.Decode(&msg)

			BeforeItem := CreateItem(msg.Payload.Before, _resource.Schema())
			AfterItem := CreateItem(msg.Payload.After, _resource.Schema())

			switch msg.Payload.Op {
			case "c":
				err := _resource.Insert(context.Background(), []*resource.Item{AfterItem})
        		if err != nil {
        			log.Fatalf("Insert Error: %s", err)
        		}	
			case "u":
				err := _resource.Update(context.Background(), AfterItem, BeforeItem)
        		if err != nil {
        			log.Fatalf("Update Error: %s", err)
        		}	
			case "d":
				err := _resource.Delete(context.Background(), BeforeItem)
        		if err != nil {
        			log.Fatalf("Delete Error: %s", err)
        		}	
			}
		}
	}()

	for _,r := range _resource.GetResources() {
		SyncResource(_readerConfig, r)
	}
}

func CreateItem(payload *map[string]interface{}, schema schema.Schema) *resource.Item {
	if payload == nil {
		return nil
	}

	p := *payload

	schema.Compile(nil)

	for fieldName,field := range schema.Fields {
		value, err := field.Validator.Validate(p[fieldName])
		if err != nil {
			switch err.Error() {
			case "not a time": 
				epoch,_ := p[fieldName].(json.Number).Int64()
				value = time.Unix(0, epoch*1000).UTC()
			default:
				log.Println("Field Error:", err)
			}
		}

		p[fieldName] = value
	}

	Etag := p["etag"].(string)
	delete(p, "etag")

	return &resource.Item {
		ID: p["id"],
		ETag: Etag,
		Updated: p["updated"].(time.Time),
		Payload: p,
	}
}