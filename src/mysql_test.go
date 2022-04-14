package main

import (
	"fmt"
	"testing"
)

// https://books.studygolang.com/The-Golang-Standard-Library-by-Example/chapter09/09.1.html
func TestMyDBPool(t *testing.T) {
	db := myDBPool("mc", "Mc@654321",
		"localhost",
		3306, "test", "utf8mb4")
	defer db.Close()
	rows, _ := db.Query("select id from closure_table_user")
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
	db := myDBPool("mc", "Mc@654321",
		"localhost",
		3306, "test", "utf8mb4")
	defer db.Close()
	result := Insert(db, "INSERT INTO `wx_user`(`openid`, `unionid`, `nick_name`, `avatar_url`, `gender`) VALUES ('openid001', 'unionid001', 'name001', 'avatar001', 0)")
	lastInsertID, _ := result.LastInsertId()
	affected, _ := result.RowsAffected()
	fmt.Printf("last insert id: %v, affected rows: %v.\n", lastInsertID, affected)
}

func TestBuildInsertSql(t *testing.T) {
	fields := []string{"openid", "unionid", "nick_name", "avatar_url", "gender"}
	values := "(\"o4vAx5fX1tGLhwYE3YQleBnjePNk\",\"oSCbM6kY4k7Hrnlh99ytx5x9cVS4\",\"黄卫\",\"https://thirdwx.qlogo.cn/mmopen/vi_32/DYAIOgq83eoJXB1uYXWH6WKNJhR2YFbeHKeC9rCOVyvwtZdibmACHc1CR565C1SaqKPibUaicewmDDfsBAlIZpfKg/132\",0)"
	insertSql := BuildInsertSql("wx_user", fields, values, "openid")
	fmt.Printf("insert sql: %s.\n", insertSql)
}
