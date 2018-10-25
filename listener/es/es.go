package es

import (
	"context"
	"os"

	"github.com/olivere/elastic"
)

const mapping = `{
	"mappings":{
	   "audit":{
		  "properties":{
			 "id":{
				"type":"long"
			 },
			 "correlation_id":{
				"type":"long"
			 },
			 "module":{
				"type":"keyword"
			 },
			 "action":{
				"type":"keyword"
			 },
			 "login":{
				"type":"text"
			 },
			 "entity":{
				"type":"keyword"
			 },
			 "ip":{
				"type":"ip"
			 },
			 "transaction_at":{
				"type":"date"
			 },
			 "location":{
				"type":"geo_point"
			 }
		  }
	   }
	}
 }`

//ES instance of elastic search client
type ES struct {
	client *elastic.Client
}

//CreateIndex create a new index in ES
func (es ES) createIndex() {
	exists, err := es.client.IndexExists("audit").Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !exists {
		_, err = es.client.CreateIndex("audit").BodyString(mapping).Do(context.Background())
		if err != nil {
			panic(err)
		}
	}
}

//Send data to ES
func (es ES) Send(body []byte) {

	_, err := es.client.Index().
		Index("audit").
		Type("audit").
		BodyString(string(body)).
		Do(context.Background())

	if err != nil {
		panic(err)
	}

}

func newClient() *elastic.Client {
	url := os.Getenv("ELASTICSEARCH_URL")
	if len(url) == 0 {
		url = elastic.DefaultURL
	}
	client, err := elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		panic(err)
	}
	return client
}

// New create a new instance of ES
func New() ES {
	es := ES{
		client: newClient(),
	}
	es.createIndex()
	return es
}
