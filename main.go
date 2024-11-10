package main

import (
	consumer "KafkaGoDemo/consumer"
	"KafkaGoDemo/producer"
	"context"
	"sync"
	_ "time"
)

func main() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(1)
	go consumer.ListenSignal(ctx, &wg)

	//增加一个等待的goroutine
	wg.Add(1)
	go producer.WriteMessages(ctx, &wg)

	wg.Add(1)
	go consumer.ReadMessages(ctx, &wg)

	wg.Wait()
}
