package main

import (
	"log"
	"time"
)
import "github.com/nsqio/go-nsq"

const topic = "testAlvin"

var handle = nsq.HandlerFunc(func(message *nsq.Message) error {
	log.Println(string(message.Body))
	return nil
})

func main() {
	var err error
	config := nsq.NewConfig()
	config.HeartbeatInterval = 30 * time.Second

	producer, err := nsq.NewProducer("127.0.0.1:4150", config)
	if err != nil {
		log.Fatalln(err)
	}
	err = producer.Ping()
	if err != nil {
		log.Fatalln(err)
	}

	go func() {
		consumer, err := nsq.NewConsumer(topic, "test1", config)
		if err != nil {
			log.Fatalln(err)
		}

		consumer.AddHandler(handle)

		if err := consumer.ConnectToNSQD("127.0.0.1:4150"); err != nil {
			log.Fatal(err)
		}

		time.Sleep(20 * time.Second)
		consumer.Stop()
		i := <-consumer.StopChan
		log.Println("test1 close", i)
	}()

	go func() {
		consumer, err := nsq.NewConsumer(topic, "test2", config)
		if err != nil {
			log.Fatalln(err)
		}

		consumer.AddHandler(handle)

		if err := consumer.ConnectToNSQD("127.0.0.1:4150"); err != nil {
			log.Fatal(err)
		}

		time.Sleep(60 * time.Second)
		consumer.Stop()
		i := <-consumer.StopChan
		log.Println("test1 close", i)

		conn := nsq.NewConn("127.0.0.1:4150", config)
		if err := conn.WriteCommand(nsq.UnRegister(topic, "test2")); err != nil {
			log.Fatal(err)
		}

	}()

	//for {
	//	time.Sleep(time.Second)
	//	log.Println("publish",producer.Publish(topic,[]byte("hello nsq")))
	//}

	time.Sleep(3 * time.Second)

}
