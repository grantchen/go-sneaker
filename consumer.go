package sneaker

import (
	"github.com/imdario/mergo"
	"github.com/streadway/amqp"
)

type Consumer struct {
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	ExchangeName string
}

type fn func([]byte)

func NewConsumer(amqpUrl, exchangeName string) *Consumer {
	amqpConn, err := amqp.Dial(amqpUrl)
	if err != nil {
		panic("failed to connect rabbitMQ")
	}
	channel, err := amqpConn.Channel()
	if err != nil {
		panic("failed to init rabbitMQ Channel")
	}
	if exchangeName == "" {
		exchangeName = "go_sneaker"
	}
	consumer := Consumer{Connection: amqpConn,
		Channel: channel, ExchangeName: exchangeName}
	return &consumer
}

// consume a worker queue
// exchaneName - exchange Name
// queueName - queueName
func (c *Consumer) Consume(queueName string, args map[string]interface{}, f fn) {
	defaultArgs := map[string]interface{}{
		"durable": true, "autoDelete": false, "autoAck": false,
		"exclusive": false, "noWait": false,
		"noLocal": false, "consumer": "", "threads": 5}
	if err := mergo.Merge(&defaultArgs, args, mergo.WithOverride); err != nil {
		panic(err)
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
		panic(err)
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
		queue.Name,     // queue name
		queue.Name,     // routing key
		c.ExchangeName, // exchange
		false,
		nil)
	if err != nil {
		panic(err)
	}

	msgs, err := c.Channel.Consume(
		queue.Name, defaultArgs["consumer"].(string),
		defaultArgs["autoAck"].(bool), defaultArgs["exclusive"].(bool),
		defaultArgs["noLocal"].(bool), defaultArgs["noWait"].(bool),
		nil)
	if err != nil {
		panic(err)
	}
	go func() {
		defer c.Connection.Close()
		defer c.Channel.Close()
		forever := make(chan bool)
		threadCount := defaultArgs["threads"].(int)
		for i := 0; i < threadCount; i++ {
			go func() {
				for msg := range msgs {
					f(msg.Body)
					if !defaultArgs["autoAck"].(bool) {
						msg.Ack(false)
					}
				}
			}()
		}
		<-forever
	}()
}
