package main

import (
	"database/sql"
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/streadway/amqp"
	"github.com/tidwall/gjson"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func consumeMsg(forever chan bool, dbpools []*sql.DB, ch *amqp.Channel, queueName string, listener []InstanceInfo) {
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer, queue name: "+queueName+".")
	go func() {
		for msg := range msgs {
			for index, dbpool := range dbpools {
				listenerInstance := listener[index]
				tableName := listenerInstance.Table
				uniKey := listenerInstance.UniKey
				fieldMap := listenerInstance.FieldMap
				ignoreList := listenerInstance.Ignore
				var strFields []string
				var strValues []string
				// strMsg := string(msg.Body)
				// log.Printf("receive msg: %s.\n", strMsg)
				head := gjson.GetBytes(msg.Body, "head")
				if head.Exists() {
					headMap := head.Map()
					if headMap != nil {
						reqId := headMap["req_id"]
						log.Printf("reqId: %v.\n", reqId)
					} else {
						log.Printf(" head is nil.\n")
					}
				} else {
					log.Printf("head does not exist.\n")
				}

				body := gjson.GetBytes(msg.Body, "body")
				if body.Exists() {
					bodyMap := body.Map()
					if bodyMap != nil {
						data := bodyMap["data"]
						if data.Exists() {
							dataArray := data.Array()
							for _, item := range dataArray {
								itemMap := item.Map()
								valuesArray := itemMap["values"].Array()
								fieldsArray := itemMap["fields"].Array()
								if len(valuesArray) == 0 || len(fieldsArray) == 0 {
									continue
								}
								strFields = make([]string, len(fieldsArray))
								strValues = make([]string, len(fieldsArray))
								fieldIndex := 0
								for index, field := range fieldsArray {
									strField := field.String()
									ignoreField := false
									for _, ignore := range ignoreList {
										if ignore == strField {
											ignoreField = true
											break
										}
									}
									if ignoreField {
									} else {
										if strToField, ok := fieldMap[strField]; ok {
											strFields[fieldIndex] = strToField
										} else {
											strFields[fieldIndex] = strField
										}
										valueItem := valuesArray[index]
										strValue := ""
										if valueItem.Type == gjson.String {
											strValue = "'" + valueItem.String() + "'"
										} else if valueItem.Type == gjson.Null {
											strValue = valueItem.Raw
										} else {
											strValue = valueItem.String()
										}
										strValues[fieldIndex] = strValue
										fieldIndex++
									}
								}
								strFields = strFields[0:fieldIndex]
								strValues = strValues[0:fieldIndex]
								// if len(strValues) > 0 {
								// 	strValues += ","
								// }
								// strValues += "(" + values.Raw[1:len(values.Raw)-1] + ")"
							}
						} else {
							log.Printf("data does not exist.\n")
						}
					} else {
						log.Printf("body is nil.\n")
					}
				} else {
					log.Printf("body does not exist.\n")
				}

				log.Printf("receive a message, delivery tag: %d. \n", msg.DeliveryTag)
				insertSql := BuildInsertSql(tableName, strFields, "("+strings.Join(strValues, ",")+")", uniKey)
				log.Printf("insert sql: %v.\n", insertSql)
				result := Insert(dbpool, insertSql)
				if result != nil {
					lastInsertID, _ := result.LastInsertId()
					affectedRows, _ := result.RowsAffected()
					log.Printf("last insert ID: %v, affected rows: %v.", lastInsertID, affectedRows)
				}
			}
			msg.Ack(false)
		} // for
		// flush cached data to db
		// log.Println("flush cached data to db.")
		// delivery := <-msgs
		// delivery.Ack(true)
		// log.Printf("Ack a message, delivery tag: %d.\n", delivery.DeliveryTag)
	}()
	log.Printf(" [*] Awaiting RPC requests.\n")
	<-forever
	log.Printf(" [*] Aborting RPC requests.\n")
}

func consume(forever chan bool, dbpools []*sql.DB, ch *amqp.Channel, queueName string, listener []InstanceInfo, durable bool) {
	_, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1024,  // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	go consumeMsg(forever, dbpools, ch, queueName, listener)
}

func main() {
	var cfgFile = flag.String("conf", "conf/config.yml", "Input config file.")
	flag.Parse()
	filename, err := filepath.Abs(*cfgFile)
	failOnError(err, "Failed get config filename.")
	cfg := LoadConfig(filename)
	log.Printf("App version: %s.", cfg.App.Version)

	// rabbitmq
	// url特殊字符转换
	brokerURL := "amqp://" + cfg.RabbitMQ.Username + ":" + url.QueryEscape(cfg.RabbitMQ.Password) + "@" + cfg.RabbitMQ.Host + ":" + cfg.RabbitMQ.Port
	log.Printf("broker: %s.", brokerURL)
	conn, err := amqp.Dial(brokerURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	mainForever := make(chan bool)

	channel, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer channel.Close()
	foreverChan := make(chan bool)
	// MySQL
	instanceSize := len(cfg.Listener)
	dbPools := make([]*sql.DB, instanceSize)
	for index, instance := range cfg.Listener {
		if instance.Type == "mysql" {
			dbpool := myDBPool(instance.Username,
				instance.Password, instance.Host, instance.Port, instance.Db, instance.Charset)
			defer dbpool.Close()
			dbPools[index] = dbpool
		} else {
			// other listener
		}
	}
	consume(foreverChan, dbPools, channel, cfg.RabbitMQ.Queue, cfg.Listener, cfg.RabbitMQ.Durable)

	// no delete consumption for the present.
	// chdel, err := conn.Channel()
	// failOnError(err, "Failed to open a channel")
	// defer chdel.Close()
	// channels[2*i+1] = chdel
	// consume(dbpool, chdel, formatQueueName+".del")

	// graceful exit server in go
	// https://guzalexander.com/2017/05/31/gracefully-exit-server-in-go.html
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		select {
		case sig := <-c:
			log.Printf("Got %s signal. Aborting...\n", sig)
			foreverChan <- true
			time.Sleep(500 * time.Millisecond)
			mainForever <- true
		}
	}()
	time.Sleep(50 * time.Millisecond)
	log.Printf(" [*] Awaiting all jobs done.\n")
	<-mainForever
	log.Printf(" [*] All jobs done.\n")
	channel.Close()
	log.Printf(" [*] Rabbitmq channels closed.\n")
	conn.Close()
	log.Printf(" [*] Rabbitmq connection closed.\n")
	log.Printf(" [*] Program exited.\n")
}
