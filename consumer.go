package sneaker

import (
	"errors"
	"github.com/imdario/mergo"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type Consumer struct {
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	ExchangeName string
	url          string     // url of the rabbitmq server
	closedError  chan error // connection closed error
	consumers    *consumers // all queues consumers
}

type fn func([]byte)

type consumers struct {
	chans map[string]consumerBuffers
	sync.Mutex
}

type consumerBuffers struct {
	queueName  string
	args       map[string]interface{}
	f          fn
	deliveries <-chan amqp.Delivery
}

// add consumed queues to consumers
func (subs *consumers) add(queueName string, args map[string]interface{}, f fn, deliveries <-chan amqp.Delivery) {
	subs.Lock()
	defer subs.Unlock()

	subs.chans[queueName] = consumerBuffers{
		queueName:  queueName,
		args:       args,
		f:          f,
		deliveries: deliveries,
	}
}

func NewConsumer(amqpUrl, exchangeName string) (*Consumer, error) {
	if exchangeName == "" {
		exchangeName = "go_sneaker"
	}

	consumer := Consumer{
		Connection:   nil,
		Channel:      nil,
		ExchangeName: exchangeName,
		url:          amqpUrl,
		closedError:  make(chan error),
		consumers: &consumers{
			chans: make(map[string]consumerBuffers),
		},
	}

	if err := consumer.Connect(); err != nil {
		return nil, err
	}

	// recover connection and consumers
	consumer.AutoRecover()

	return &consumer, nil
}

// Connect server
func (c *Consumer) Connect() error {
	var err error

	c.Connection, err = amqp.Dial(c.url)
	if err != nil {
		return err
	}

	go func() {
		// add closed error if the channel to be closed
		<-c.Connection.NotifyClose(make(chan *amqp.Error))
		c.closedError <- errors.New("connection closed")
	}()

	c.Channel, err = c.Connection.Channel()
	if err != nil {
		return err
	}

	return c.Channel.ExchangeDeclare(
		c.ExchangeName, // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
}

// ReConnect server and re-consume all queues
func (c *Consumer) ReConnect() error {
	if err := c.Connect(); err != nil {
		return err
	}

	for queueName, buffers := range c.consumers.chans {
		err := c.Consume(queueName, buffers.args, buffers.f)
		if err != nil {
			return err
		}
	}

	return nil
}

// AnnounceQueue declares and consume a queue
func (c *Consumer) AnnounceQueue(queueName string, args map[string]interface{}) (<-chan amqp.Delivery, error) {
	queue, err := c.Channel.QueueDeclare(
		queueName,                 // name
		args["durable"].(bool),    // durable
		args["autoDelete"].(bool), // delete when unused
		args["exclusive"].(bool),  // exclusive
		args["noWait"].(bool),     // no-wait
		nil,                       // arguments
	)

	err = c.Channel.QueueBind(
		queue.Name,     // queue name
		queue.Name,     // routing key
		c.ExchangeName, // exchange
		false,
		nil)
	if err != nil {
		return nil, err
	}

	deliveries, err := c.Channel.Consume(
		queue.Name,
		args["consumer"].(string),
		args["autoAck"].(bool),
		args["exclusive"].(bool),
		args["noLocal"].(bool),
		args["noWait"].(bool),
		nil)
	return deliveries, err
}

// Consume consume a worker queue
func (c *Consumer) Consume(queueName string, args map[string]interface{}, f fn) error {
	var err error
	if args, err = mergeDefaultArgs(args); err != nil {
		return err
	}

	deliveries, err := c.AnnounceQueue(queueName, args)
	if err != nil {
		return err
	}

	c.consumers.add(queueName, args, f, deliveries)

	go func() {
		defer c.Connection.Close()
		defer c.Channel.Close()
		forever := make(chan bool)
		threadCount := args["threads"].(int)
		for i := 0; i < threadCount; i++ {
			go func() {
				for msg := range deliveries {
					f(msg.Body)
					if !args["autoAck"].(bool) {
						msg.Ack(false)
					}
				}
			}()
		}
		<-forever
	}()

	return nil
}

// AutoRecover will recover connection and consumers
func (c *Consumer) AutoRecover() {
	go func() {
		for {
			// reconnect when c.closedError is passed non nil values
			if <-c.closedError != nil {
				time.Sleep(10 * time.Second)
				_ = c.ReConnect()
			}
		}
	}()
}

// merge default configs into args
func mergeDefaultArgs(args map[string]interface{}) (map[string]interface{}, error) {
	dst := map[string]interface{}{
		"durable":    true,
		"autoDelete": false,
		"autoAck":    false,
		"exclusive":  false,
		"noWait":     false,
		"noLocal":    false,
		"consumer":   "",
		"threads":    5}
	if err := mergo.Merge(&dst, args, mergo.WithOverride); err != nil {
		return nil, err
	}

	return dst, nil
}
