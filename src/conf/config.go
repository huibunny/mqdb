package conf

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

// https://github.com/go-yaml/yaml
// https://github.com/go-yaml/yaml/blob/v2/example_embedded_test.go
// https://www.cnblogs.com/didispace/p/12524194.html

type (
	App struct {
		Version    string `yaml:"version"`
		RoutineNum int    `yaml:"routinenum"`
	}

	InstanceInfo struct {
		Type     string            `yaml:"type"`
		Host     string            `yaml:"host"`
		Port     int               `yaml:"port"`
		Username string            `yaml:"username"`
		Password string            `yaml:"password"`
		Db       string            `yaml:"db"`
		Charset  string            `yaml:"charset"`
		Table    string            `yaml:"table"`
		UniKey   string            `yaml:"unikey"`
		Ignore   []string          `yaml:"ignore"`
		FieldMap map[string]string `yaml:"fieldmap"`
	}

	// Log -.
	Log struct {
		Level string `env-required:"true" yaml:"level"   env:"LEVEL"`
		File  string `yaml:"file"`
	}

	RabbitMQ struct {
		Host         string `yaml:"host"`
		Port         string `yaml:"port"`
		Username     string `yaml:"username"`
		Password     string `yaml:"password"`
		VirtualHost  string `yaml:"virtualhost"`
		Exchange     string `yaml:"exchange"`
		Queue        string `yaml:"queue"`
		ExchangeTyep string `yaml:"exchangetype"`
		Routingkey   string `yaml:"routingkey"`
	}

	// YmlConfig structure definition
	YmlConfig struct {
		App      `yaml:"app"`
		Log      `yaml:"logger"`
		RabbitMQ `yaml:"rabbitmq"`
		Listener []InstanceInfo `yaml:"listener"`
	}
)

// LoadConfig load yaml file to json string
func LoadConfig(filename string) YmlConfig {
	f, err := os.Open(filename)
	if err != nil {
		fmt.Println("err", err)
	}
	defer f.Close()

	var cfg YmlConfig
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		fmt.Println("err", err)
	}

	return cfg
}
