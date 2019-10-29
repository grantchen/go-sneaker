package sneaker

import (
	"github.com/streadway/amqp"
)

type Publisher struct {
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	ExchangeName string
}

func NewPublisher(amqpUrl, exchangeName string) (*Publisher, error) {
	amqpConn, err := amqp.Dial(amqpUrl)
	if err != nil {
		return nil, err
	}

	channel, err := amqpConn.Channel()
	if err != nil {
		return nil, err
	}
	publisher := Publisher{Connection: amqpConn, Channel: channel, ExchangeName: exchangeName}
	return &publisher, nil
}

// publish a worker queue
func (c *Publisher) Publish(queueName, bodyContentType string, body []byte) error {
	if bodyContentType == "" {
		bodyContentType = "text/json"
	}
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
		return err
	}
	err = c.Channel.Publish(
		c.ExchangeName, // exchange
		queueName,      // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: bodyContentType,
			Body:        body,
		})
	if err != nil {
		return err
	}
	return nil
}

func (c *Publisher) Close() {
	c.Channel.Close()
	c.Connection.Close()
}
