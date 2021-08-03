package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const workerPoolSize = 4

func main() {
	// создание пользователя
	consumer := Consumer{
		ingestChan: make(chan int, 1),
		jobsChan:   make(chan int, workerPoolSize),
	}

	// Симуляция отправки ивентов
	producer := Producer{callbackFunc: consumer.callbackFunc}
	go producer.start()

	// Контекст для завершения
	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// Запуск пользователя
	go consumer.startConsumer(ctx)

	// старт работников в WaitGroup
	wg.Add(workerPoolSize)
	for i := 0; i < workerPoolSize; i++ {
		go consumer.workerFunc(wg, i)
	}

	// отлавливаем синал
	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-termChan         // блокировка до прерывания

	// Handle shutdown
	fmt.Println("*********************************\nShutdown signal received\n*********************************")
	cancelFunc()       // завершение сигнала
	wg.Wait()          // блокировка WaitGroup до конца работы всех каналов
	
	fmt.Println("All workers done, shutting down!")
}

// -- пользователь
type Consumer struct {
	ingestChan chan int
	jobsChan   chan int
}

// callbackFunc
func (c Consumer) callbackFunc(event int) {
	c.ingestChan <- event
}

// workerFunc 
func (c Consumer) workerFunc(wg *sync.WaitGroup, index int) {
	defer wg.Done()

	fmt.Printf("Worker %d starting\n", index)
	for eventIndex := range c.jobsChan {
		// симуляция работы со случайным временем
		fmt.Printf("Worker %d started job %d\n", index, eventIndex)
		time.Sleep(time.Millisecond * time.Duration(1000+rand.Intn(2000)))
		fmt.Printf("Worker %d finished processing job %d\n", index, eventIndex)
	}
	fmt.Printf("Worker %d interrupted\n", index)
}

// startConsumer 
func (c Consumer) startConsumer(ctx context.Context) {
	for {
		select {
		case job := <-c.ingestChan:
			c.jobsChan <- job
		case <-ctx.Done():
			fmt.Println("Consumer received cancellation signal, closing jobsChan!")
			close(c.jobsChan)
			fmt.Println("Consumer closed jobsChan")
			return
		}
	}
}

// Producer 
type Producer struct {
    callbackFunc func(event int)
}
func (p Producer) start() {
    eventIndex := 0
    for {
        p.callbackFunc(eventIndex)
        eventIndex++
        time.Sleep(time.Millisecond * 100)
    }
}