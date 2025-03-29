package config

import (
	"fmt"
	"log"
	"os"
)

func GetEnv(key string) string {
	fmt.Println("Fetching environment variables...")
	value, exists := os.LookupEnv(key)
	if !exists {
		log.Fatal("Environment Variable not set", key)
	}
	return value
}
