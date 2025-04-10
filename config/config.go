package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type MySQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
}

type PostgreSQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
}

// config struct to map config.yaml
type Config struct {
	MySQL      MySQLConfig      `yaml:"mysql"`
	PostgreSQL PostgreSQLConfig `yaml:"postgresql"`
	FilePath   string           `yaml:"file_path"`
}

func LoadConfig(filepath string) (*Config, error) {

	content, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file, %v", err)
	}

	var config Config
	err = yaml.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %v", err)
	}
	return &config, nil
}
