package sneaker

import (
	"github.com/imdario/mergo"
	"github.com/streadway/amqp"
)

type Worker struct {
	Channel *amqp.Channel
}

type fn func([]byte)

func NewWorker(amqpUrl string) *Worker {
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

// consume a worker queue
func (c *Worker) Consume(exchangeName string,
	queueName string, args map[string]interface{}, f fn) {
	defaultArgs := map[string]interface{}{
		"durable": true, "autoDelete": false,
		"exclusive": false, "noWait": false,
		"noLocal": false, "consumer": ""}
	if err := mergo.Merge(&defaultArgs, args, mergo.WithOverride); err != nil {
		panic("Failed to Merge args")
	}
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

	queue, err := c.Channel.QueueDeclare(
		queueName,                        // name
		defaultArgs["durable"].(bool),    // durable
		defaultArgs["autoDelete"].(bool), // delete when unused
		defaultArgs["exclusive"].(bool),  // exclusive
		defaultArgs["noWait"].(bool),     // no-wait
		nil, // arguments
	)

	err = c.Channel.QueueBind(
		queue.Name,   // queue name
		queue.Name,   // routing key
		exchangeName, // exchange
		false,
		nil)
	if err != nil {
		panic("Failed to bind a queue")
	}

	msgs, err := c.Channel.Consume(
		queue.Name, defaultArgs["consumer"].(string),
		defaultArgs["autoAck"].(bool), defaultArgs["exclusive"].(bool),
		defaultArgs["noLocal"].(bool), defaultArgs["noWait"].(bool),
		nil)
	if err != nil {
		panic("Consume Worker Error: %v")
	}
	forever := make(chan bool)
	go func() {
		for msg := range msgs {
			f(msg.Body)
		}
	}()
	<-forever
}
