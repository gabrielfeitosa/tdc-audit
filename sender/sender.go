package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

func random(a []string) string {
	return a[rand.Intn(len(a))]
}

//Audit auditable structure
type Audit struct {
	ID            int64     `json:"id"`
	IDCorrelation int       `json:"correlation_id"`
	Module        string    `json:"module"`
	Action        string    `json:"action"`
	Login         string    `json:"login"`
	TransactionAt time.Time `json:"transaction_at"`
	Entity        string    `json:"entity"`
}

func getCustomAudit() Audit {
	modules := []string{"sysmanca", "systoca", "sysliga"}
	actions := []string{"create", "update", "delete"}
	logins := []string{"fulano", "beltrano", "sicrano"}
	entities := []string{"user", "profile", "item"}

	return Audit{
		ID:            rand.Int63n(100),
		IDCorrelation: rand.Intn(20),
		Login:         random(logins),
		Action:        random(actions),
		Module:        random(modules),
		TransactionAt: time.Now(),
		Entity:        random(entities),
	}
}
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"audit", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")
	for {
		body, _ := json.Marshal(getCustomAudit())
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			})
		log.Printf(" [x] Sent %s", string(body))
		failOnError(err, "Failed to publish a message")

		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	}
}
