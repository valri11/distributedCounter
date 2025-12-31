Download SQl connector for flink

```
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
```

run infra
```
docker compose -f docker-compose-kafka-flink-infra.yml up
```

run resource server
```
./distributedcounter --config=app.yml resourceserver
```

run load
```
cd k6
source ./init_env
k6 run ./smoke-test.js
```


run flink sql clinet
```
docker-compose -f docker-compose-kafka-flink-infra.yml run sql-client
```

and execute next statements:

```
CREATE TABLE kafka_input (
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
);

SELECT 
    account_id, 
    SUM(counter) as total_count
FROM kafka_input
GROUP BY account_id;
```
