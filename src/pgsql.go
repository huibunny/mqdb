package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
)

// database_url:  "postgresql://fvs:123456@localhost:5432/fvs"
// basic type mapping between golang and pgsql:
//            https://gowalker.org/github.com/jackc/pgx
func pgDBPool(url string) (dbpool *pgxpool.Pool) {
	dbpool, err := pgxpool.Connect(context.Background(), url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	} else {
		fmt.Fprintln(os.Stdout, "connect to pgsql successfully.")
	}

	return dbpool
}

