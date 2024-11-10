package producer

import (
	"context"
	"database/sql"
	"github.com/segmentio/kafka-go"
	"kafkademo/logger"
	"sync"
	"time"
)

var (
	topic = "user_click"
)

func WriteMessages(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	writer := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		WriteTimeout:           1 * time.Second,
		RequiredAcks:           kafka.RequireNone,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/kfktest")
	if err != nil {
		logger.Error.Println("连接数据库失败:", err)
		return
	}
	defer db.Close()

	for i := 0; i < 3; i++ {
		if err := writer.WriteMessages(
			ctx,
			kafka.Message{Key: []byte("1"), Value: []byte("万幸")},
			kafka.Message{Key: []byte("2"), Value: []byte("得以")},
			kafka.Message{Key: []byte("3"), Value: []byte("相逢")},
		); err != nil {
			if err == kafka.LeaderNotAvailable {
				time.Sleep(10 * time.Second)
				continue
			} else {
				logger.Error.Println("批量写kafka失败:", err)
			}
		} else {
			logger.Info.Println("写入消息成功")
		}
	}
}
