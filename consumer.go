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

func NewConsumer(amqpUrl, exchangeName string) (*Consumer, error) {
	amqpConn, err := amqp.Dial(amqpUrl)
	if err != nil {
		return nil, err
	}
	channel, err := amqpConn.Channel()
	if err != nil {
		return nil, err
	}
	if exchangeName == "" {
		exchangeName = "go_sneaker"
	}
	consumer := Consumer{Connection: amqpConn,
		Channel: channel, ExchangeName: exchangeName}
	return &consumer, nil
}

// consume a worker queue
// exchaneName - exchange Name
// queueName - queueName
func (c *Consumer) Consume(queueName string, args map[string]interface{}, f fn) error {
	defaultArgs := map[string]interface{}{
		"durable": true, "autoDelete": false, "autoAck": false,
		"exclusive": false, "noWait": false,
		"noLocal": false, "consumer": "", "threads": 5}
	if err := mergo.Merge(&defaultArgs, args, mergo.WithOverride); err != nil {
		return err
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
		return err
	}

	msgs, err := c.Channel.Consume(
		queue.Name, defaultArgs["consumer"].(string),
		defaultArgs["autoAck"].(bool), defaultArgs["exclusive"].(bool),
		defaultArgs["noLocal"].(bool), defaultArgs["noWait"].(bool),
		nil)
	if err != nil {
		return err
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
	return nil
}
