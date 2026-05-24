package configuration

import (
	"encoding/json"
	"os"
)

type MongoConfig struct {
	Uri                string `json:"uri"`
	Database           string `json:"database"`
	MessagesCollection string `json:"messagesCollection"`
	UsersCollection    string `json:"usersCollection"`
	SessionsCollection string `json:"sessionsCollection"`
	ProductsCollection string `json:"productsCollection"`
	SocketRoute        string `json:"socketRoute"`
	UUIDNamespace      string `json:"uuid_namespace"`
}

type DatabaseConfig struct {
	Dsn string `json:"dsn"`
}

type ServerConfig struct {
	AppPort    int `json:"app_port"`
	SocketPort int `json:"socket_port"`
}

type RedisConfig struct {
	Addr     string `json:"addr"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

type Config struct {
	Database     DatabaseConfig `json:"mysql"`
	ChatDatabase MongoConfig    `json:"mongo"`
	Server       ServerConfig   `json:"server"`
	Redis        RedisConfig    `json:"redis"`
}

func LoadConfig(config_path string) (*Config, error) {
	file, err := os.ReadFile(config_path)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(file, &config)
	if err != nil {
		return nil, err
	}

	// Environment variable overrides for production/deployment
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		config.ChatDatabase.Uri = uri
	}
	if dbName := os.Getenv("MONGO_DB"); dbName != "" {
		config.ChatDatabase.Database = dbName
	}
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		config.Redis.Addr = redisAddr
	}
	if redisPass := os.Getenv("REDIS_PASSWORD"); redisPass != "" {
		config.Redis.Password = redisPass
	}
	if socketPort := os.Getenv("SOCKET_PORT"); socketPort != "" {
		// Simple way to handle port override if needed
		// config.Server.SocketPort = ... (needs conversion)
	}

	return &config, nil
}
