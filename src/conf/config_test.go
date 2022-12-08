package conf

import (
	"fmt"
	"log"
	"path/filepath"
	"testing"
)

// https://books.studygolang.com/The-Golang-Standard-Library-by-Example/chapter09/09.1.html
/*
app:
    version: v1.0
    routinenum: 8
rabbitmq:
        host: localhost
        port: 5672
        username: test
        password: test
        virtualhost: /
        exchange: test.queue.exchange
        queue: test.queue
        exchangetype: direct
        routingkey: test.queue.key
listener:
        -
                type: mysql
                host: localhost
                port: 3306
                username: test
                password: test
                db: test
                charset: utf8mb4
                table: user
                unikey: openid
        -
                type: mysql
                host: localhost
                port: 3306
                username: test
                password: test
                db: student
                charset: utf8mb4
                table: student_info
                unikey: openid
                ignore: [ unionid, ]
                fieldmap:
                        nick_name: stu_nick

*/
func TestLoadConfig(t *testing.T) {
	filename, err := filepath.Abs("conf/config.yml")
	if err != nil {
		log.Fatalf("Failed get config filename.: %s", err)
	}
	cfg := LoadConfig(filename)
	fmt.Printf("version: %v.\n", cfg.App.Version)
	for _, listener := range cfg.Listener {
		fmt.Printf("listener: \n")
		fmt.Printf("  type: %v.\n", listener.Type)
		fmt.Printf("  host: %v.\n", listener.Host)
		fmt.Printf("  port: %v.\n", listener.Port)
		fmt.Printf("  username: %v.\n", listener.Username)
		fmt.Printf("  password: %v.\n", listener.Password)
		fmt.Printf("  db: %v.\n", listener.Db)
		fmt.Printf("  charset: %v.\n", listener.Charset)
		fmt.Printf("  table: %v.\n", listener.Table)
		fmt.Printf("  unikey: %v.\n", listener.UniKey)
		fmt.Printf("  ignore: %v.\n", listener.Ignore)
		fmt.Printf("  fieldmap:\n")
		for key, value := range listener.FieldMap {
			fmt.Printf("    %v: %v.\n", key, value)
		}
	}
}
