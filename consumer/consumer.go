package consumer

import (
	"KafkaGoDemo/logger"
	"context"
	"database/sql"
	"github.com/segmentio/kafka-go"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	topic = "user_click"
)

func ReadMessages(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          topic,
		CommitInterval: 1 * time.Second,
		GroupID:        "rec_team",
		StartOffset:    kafka.FirstOffset, // earliest or latest
	})

	//连接数据库
	db, err := sql.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/kfktest")
	if err != nil {
		logger.Error.Println("连接数据库失败:", err)
		return
	}
	defer db.Close()

	for {
		select {
		case <-ctx.Done():
			//当上下文被取消时，ctx.Done() 返回的 channel 会被关闭，从而触发 select 语句的 case 分支
			logger.Info.Println("上下文已取消")
			return
		default:
			if message, err := reader.ReadMessage(ctx); err != nil {
				logger.Error.Println("读取消息失败:", err)
				return
			} else {
				logger.Info.Println("读取消息成功:", message.Value)
				logger.Info.Printf("topic=%s, partition=%d, offset=%d, key=%s, value=%s", message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
			}
		}
	}
}

func ListenSignal(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-c:
		logger.Info.Println("收到信号", sig.String())
		return
	case <-ctx.Done():
		logger.Info.Println("上下文已取消")
		return
	}
}
