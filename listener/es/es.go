package es

import (
	"context"
	"encoding/json"
	"os"
	"time"

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

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Audit struct {
	ID            int64     `json:"id"`
	IDCorrelation int       `json:"correlation_id"`
	Module        string    `json:"module"`
	Action        string    `json:"action"`
	Login         string    `json:"login"`
	TransactionAt time.Time `json:"transaction_at"`
	Entity        string    `json:"entity"`
	IP            string    `json:"ip"`
	Location      Location  `json:"location"`
}

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

	var audit Audit
	err := json.Unmarshal(body, &audit)

	if err != nil {
		panic(err)
	}

	_, err = es.client.Index().
		Index("audit").
		Type("audit").
		BodyJson(audit).
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
