package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// basic type mapping between golang and pgsql:
//            https://gowalker.org/github.com/jackc/pgx
func myDBPool(userName string, password string, host string, port int, database string, charset string) (mysqlDb *sql.DB) {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", userName, password, host, port, database, charset)
	// 打开连接失败
	var mysqlDbErr error
	mysqlDb, mysqlDbErr = sql.Open("mysql", url)
	//defer mysqlDb.Close();
	if mysqlDbErr != nil {
		log.Println("url: " + url)
		panic("数据源配置不正确: " + mysqlDbErr.Error())
	}

	// 最大连接数
	mysqlDb.SetMaxOpenConns(100)
	// 闲置连接数
	mysqlDb.SetMaxIdleConns(20)
	// 最大连接周期
	mysqlDb.SetConnMaxLifetime(100 * time.Second)

	if mysqlDbErr = mysqlDb.Ping(); nil != mysqlDbErr {
		panic("数据库链接失败: " + mysqlDbErr.Error())
	}

	return mysqlDb
}

func BuildInsertSql(tableName string, fields []string, values string, unikey string) string {
	strFields := "(`" + strings.Join(fields, "`,`") + "`)"
	insertSql := "INSERT INTO `" + tableName + "` " + strFields + " VALUES " + values
	if len(unikey) > 0 {
		insertSql += " AS t ON DUPLICATE KEY UPDATE "
		updateStr := ""
		for _, field := range fields {
			if field != unikey {
				if len(updateStr) > 0 {
					updateStr += ", "
				}
				strField := "`" + field + "`"
				updateStr += strField + "=t." + strField
			}
		}
		insertSql += updateStr
	}

	return insertSql
}

func Insert(db *sql.DB, sql string) sql.Result {
	ret, error := db.Exec(sql)
	if error != nil {
		fmt.Printf("%s.\n", error.Error())
	}
	return ret
}
