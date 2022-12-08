package db

import (
	"fmt"
	"testing"
)

// https://books.studygolang.com/The-Golang-Standard-Library-by-Example/chapter09/09.1.html
func TestMyDBPool(t *testing.T) {
	db := MyDBPool("root", "123456",
		"127.0.0.1",
		3306, "test", "utf8mb4")
	defer db.Close()
	rows, _ := db.Query("select id from wx_user")
	if rows == nil {
		t.Errorf("rows is nil; expected not nil.")
	}

	id := 0
	for rows.Next() {
		rows.Scan(&id)
		fmt.Println(id)
	}

}

func TestInsertSql(t *testing.T) {
	db := MyDBPool("root", "123456",
		"127.0.0.1",
		3306, "test", "utf8mb4")
	defer db.Close()
	sql, args := BuildInsertSql("wx_user", []string{"openid", "unionid", "nick_name", "avatar_url", "gender"}, []interface{}{"openid001", "unionid001", "nick_name001", "avatar_url001", 0}, "openid")
	result, err := db.Exec(sql, args...)
	if err != nil {
		print(err)
	}
	lastInsertID, _ := result.LastInsertId()
	affected, _ := result.RowsAffected()
	fmt.Printf("last insert id: %v, affected rows: %v.\n", lastInsertID, affected)
}

func TestBuildInsertSql(t *testing.T) {
	fields := []string{"openid", "unionid", "nick_name", "avatar_url", "gender"}
	values := []interface{}{"openid001", "unionid001", "nickname001", "avatar001", 0}
	insertSql, args := BuildInsertSql("wx_user", fields, values, "openid")
	fmt.Printf("insert sql: %s, args: %v.\n", insertSql, args)
}
