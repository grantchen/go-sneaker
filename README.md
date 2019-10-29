# go-sneaker
A fast background processing framework for Golang and RabbitMQ 


## Usage

Use go get

```go
go get github.com/grantchen/go-sneaker
```

Then import the go-sneaker package into your own code.

```go
import "github.com/grantchen/go-sneaker"
```

### Publisher

send background task code sample

```go
Publisher, err := sneaker.NewPublisher(amqp_url, exchange_key))
if err != nil {
  panic(err)
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary
dataJsonByte, _ := json.Marshal(map[string]string{"key": "value"})
err = Publisher.Publish("queue_name", "text/json", dataJsonByte)
if err != nil {
  panic(err)
}

// if not use can close it
Publisher.Close()
```

#### Publish Parameters
Publish(queueName, bodyContentType string, body []byte)

```go
queueName - Queue Name
bodyContentType - send body content type. Default is text/json
body - send data body byte
```

### Consumer

background task handle worker sample

```go
Consumer, err := sneaker.NewConsumer(amqp_url, exchange_key))
if err != nil {
  panic(err)
}

err = Consumer.Consume("queue_name", map[string]interface{}{}, handleWorker)
if err != nil {
  panic(err)
}

func handleWorker(body []byte){
  //...
}
```

#### Consume Parameters
Consume(queueName string, args map[string]interface{}, f fn)

```go
queueName - Queue Name
args - consume releated params. Deafult values:

     {"durable": true, "autoDelete": false, "autoAck": false,
		 "exclusive": false, "noWait": false,
		 "noLocal": false, "consumer": "", "threads": 5}
    
     durable - is queue durable
     autoDelete - is queue delete when unused
     autoAck - is consume auto ack
     exclusive - is queue and consume exclusive
     noWait - is queue and consume no wait
     noLocal - is consume no local
     consumer - consumer name
     threads - how many threads to handle worker method f

f - handle body method
```