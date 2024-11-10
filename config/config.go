// config/config.go
package config

import (
	"fmt"
	"github.com/spf13/viper"
)

func LoadConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
}

func GetKafkaBrokers() []string {
	return viper.GetStringSlice("kafka.brokers")
}

func GetKafkaTopic() string {
	return viper.GetString("kafka.topic")
}
