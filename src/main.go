package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"mqdb/src/conf"
	"mqdb/src/db"
	"mqdb/src/pkg/logger"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/streadway/amqp"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type FormData struct {
	Data []interface{}
}

func failOnError(log *logger.Logger, err error, msg string) {
	if err != nil {
		if log != nil {
			log.Error("%s: %s", msg, err)
		} else {
			fmt.Printf("%s: %s\n", msg, err)
		}
	}
}

func consumeMsg(log *logger.Logger, forever chan bool, dbpools []db.DB, ch *amqp.Channel, queueName string, listener []conf.InstanceInfo) {
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(log, err, "Failed to register a consumer, queue name: "+queueName+".")
	go func() {
		for msg := range msgs {
			for index, dbpool := range dbpools {
				listenerInstance := listener[index]
				if dbpool.Type == "mongo" {
					var data FormData
					// msgBody := bytes.Trim(msg.Body, "\x00\x01;")
					if err := json.Unmarshal([]byte(msg.Body), &data); err != nil {
						log.Info("unmarshal error: %v.", err)
					} else {
						uniKey := ""
						for _, param := range data.Data {
							// get & remove module id
							paramMap := param.(map[string]interface{})
							uniKey = paramMap[listenerInstance.UniKey].(string)
							delete(paramMap, listenerInstance.UniKey)
							log.Info("remove %s: %v.", listenerInstance.UniKey, uniKey)
						}
						if len(uniKey) > 0 {
							_, err := dbpool.DbConn.(*mongo.Client).Database(listenerInstance.Db).Collection(uniKey).InsertMany(context.TODO(), data.Data, options.InsertMany().SetOrdered(false))
							if err != nil {
								log.Info("error: %v.", err)
							} else {
								log.Info("flush data to mongo: %v.", data.Data)
							}
						} else {
							log.Info("empty %s.", listenerInstance.UniKey)
						}
					}
				} else {
					tableName := listenerInstance.Table
					uniKey := listenerInstance.UniKey
					fieldMap := listenerInstance.FieldMap
					ignoreList := listenerInstance.Ignore
					var strFields []string
					var strValues []interface{}
					// strMsg := string(msg.Body)
					// log.Info("receive msg: %s.\n", strMsg)
					head := gjson.GetBytes(msg.Body, "head")
					if head.Exists() {
						headMap := head.Map()
						if headMap != nil {
							reqId := headMap["req_id"]
							log.Info("reqId: %v.\n", reqId)
						} else {
							log.Info(" head is nil.\n")
						}
					} else {
						log.Info("head does not exist.\n")
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
									strFields = make([]string, len(fieldsArray))
									strValues = make([]interface{}, len(fieldsArray))
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
												strValue = valueItem.String()
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
								log.Info("data does not exist.\n")
							}
						} else {
							log.Info("body is nil.\n")
						}
					} else {
						log.Info("body does not exist.\n")
					}

					log.Info("receive a message, delivery tag: %d. \n", msg.DeliveryTag)
					insertSql, args := db.BuildInsertSql(tableName, strFields, strValues, uniKey)
					log.Info("insert sql: %v, args: %v.\n", insertSql, args)
					result, err := dbpool.DbConn.(*sql.DB).Exec(insertSql, args...)
					if err != nil {
						log.Error(err.Error())
					} else {
						lastInsertID, _ := result.LastInsertId()
						affectedRows, _ := result.RowsAffected()
						log.Info("last insert ID: %v, affected rows: %v.", lastInsertID, affectedRows)
					}
				}
			}
			msg.Ack(false)
		} // for
		// flush cached data to db
		log.Info("flush cached data to db.")
		delivery := <-msgs
		delivery.Ack(true)
		log.Info("Ack a message, delivery tag: %d.\n", delivery.DeliveryTag)
	}()
	log.Info(" [*] Awaiting RPC requests.\n")
	<-forever
	log.Info(" [*] Aborting RPC requests.\n")
}

func consume(log *logger.Logger, forever chan bool, dbpools []db.DB, ch *amqp.Channel, exchangeName string, exchangeType string, queueName string, routingkey string, listener []conf.InstanceInfo) {
	err := ch.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil)
	failOnError(log, err, "Failed to declare a exchange.")
	queue, err := ch.QueueDeclare(
		queueName,    // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		amqp.Table{}, // arguments
	)
	failOnError(log, err, "Failed to declare a queue.")

	err = ch.QueueBind(queue.Name, routingkey, exchangeName, false, nil)
	failOnError(log, err, "Fail to bind queue.")

	// err = ch.Qos(
	// 	1024,  // prefetch count
	// 	0,     // prefetch size
	// 	false, // global
	// )
	// failOnError(log, err, "Failed to set QoS")

	go consumeMsg(log, forever, dbpools, ch, queueName, listener)
}

func main() {
	var cfgFile = flag.String("conf", "conf/config.yml", "Input config file.")
	flag.Parse()
	filename, err := filepath.Abs(*cfgFile)
	failOnError(nil, err, "Failed get config filename.")
	cfg := conf.LoadConfig(filename)
	log, f := logger.New(cfg.Log.Level, cfg.Log.File, 2)
	defer f.Close()
	log.Info("App version: %s.", cfg.App.Version)

	// rabbitmq
	// url特殊字符转换
	brokerURL := "amqp://" + cfg.RabbitMQ.Username + ":" + url.QueryEscape(cfg.RabbitMQ.Password) + "@" + cfg.RabbitMQ.Host + ":" + cfg.RabbitMQ.Port
	log.Info("broker: %s.", brokerURL)
	conn, err := amqp.Dial(brokerURL)
	failOnError(log, err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	mainForever := make(chan bool)

	channel, err := conn.Channel()
	failOnError(log, err, "Failed to open a channel")
	defer channel.Close()
	foreverChan := make(chan bool)
	// MySQL
	instanceSize := len(cfg.Listener)
	dbPools := make([]db.DB, instanceSize)
	for index, instance := range cfg.Listener {
		if instance.Type == "mysql" {
			dbpool := db.MyDBPool(instance.Username,
				instance.Password, instance.Host, instance.Port, instance.Db, instance.Charset)
			defer dbpool.Close()
			dbPools[index] = db.DB{Type: instance.Type, DbConn: dbpool}
		} else {
			// other listener
			dbConn, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://"+instance.Username+":"+instance.Password+"@"+instance.Host+":"+fmt.Sprint(instance.Port)))
			failOnError(log, err, "Fail to open mongo.")
			dbPools[index] = db.DB{Type: instance.Type, DbConn: dbConn}
		}
	}
	consume(log, foreverChan, dbPools, channel, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.ExchangeTyep, cfg.RabbitMQ.Queue, cfg.RabbitMQ.Routingkey, cfg.Listener)

	// no delete consumption for the present.
	// chdel, err := conn.Channel()
	// failOnError(log, err, "Failed to open a channel")
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
			log.Info("Got %s signal. Aborting...\n", sig)
			foreverChan <- true
			time.Sleep(500 * time.Millisecond)
			mainForever <- true
		}
	}()
	time.Sleep(50 * time.Millisecond)
	log.Info(" [*] Awaiting all jobs done.\n")
	<-mainForever
	log.Info(" [*] All jobs done.\n")
	channel.Close()
	log.Info(" [*] Rabbitmq channels closed.\n")
	conn.Close()
	log.Info(" [*] Rabbitmq connection closed.\n")
	log.Info(" [*] Program exited.\n")
}
