package sneaker

import (
	// "github.com/imdario/mergo"
	"github.com/streadway/amqp"
)

type Publisher struct {
	Channel *amqp.Channel
}

func NewPublisher(amqpUrl string) *Worker {
	amqpConn, err := amqp.Dial(amqpUrl)
	defer amqpConn.Close()
	if err != nil {
		panic("failed to connect rabbitMQ")
	}

	channel, err := amqpConn.Channel()
	defer channel.Close()
	if err != nil {
		panic("failed to init rabbitMQ Channel")
	}
	worker := Worker{Channel: channel}
	return &worker
}

// publish a worker queue
func (c *Publisher) Publish(exchangeName,
	queueName, bodyContentType string, body []byte) {
	err := c.Channel.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		panic("Failed to declare an exchange")
	}
	err = c.Channel.Publish(
		exchangeName, // exchange
		queueName,    // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: bodyContentType,
			Body:        []byte(body),
		})
	panic("Failed to publish a message")
}
