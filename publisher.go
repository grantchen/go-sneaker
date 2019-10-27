package sneaker

import (
	"github.com/streadway/amqp"
)

type Publisher struct {
	Channel      *amqp.Channel
	ExchangeName string
}

func NewPublisher(amqpUrl, exchangeName string) *Publisher {
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
	publisher := Publisher{Channel: channel}
	return &publisher
}

// publish a worker queue
func (c *Publisher) Publish(queueName, bodyContentType string, body []byte) {
	err := c.Channel.ExchangeDeclare(
		c.ExchangeName, // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		panic("Failed to declare an exchange")
	}
	err = c.Channel.Publish(
		c.ExchangeName, // exchange
		queueName,      // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: bodyContentType,
			Body:        []byte(body),
		})
	panic("Failed to publish a message")
}
