package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/exness/go-flink-sql"
)

type NullString sql.NullString

type AccountSum struct {
	AccountID  sql.NullString
	TotalCount sql.NullFloat64
}

func main() {
	connector, err := flink.NewConnector(
		flink.WithGatewayURL("http://localhost:8083"),
		flink.WithProperties(map[string]string{
			"execution.runtime-mode": "STREAMING",
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ddl := `CREATE TABLE kafka_input (
    account_id STRING,
    counter DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'resourcecount',
    'properties.bootstrap.servers' = 'kafka1:9093',
	'properties.group.id' = 'flink-group',
	'properties.enable.auto.commit' = 'true',
	'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		log.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, `
SELECT
    COALESCE(account_id, ''),
    COALESCE(SUM(counter), 0.0) as total_count
FROM kafka_input
GROUP BY account_id;
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var seen int
	for rows.Next() {
		var o AccountSum
		var tc sql.NullFloat64
		if err := rows.Scan(&o.AccountID, &tc); err != nil {
			log.Fatal(err)
		}

		fmt.Printf(
			"%s - %f\n",
			o.AccountID.String,
			o.TotalCount.Float64,
		)

		seen++
		if seen == 10 {
			break
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
