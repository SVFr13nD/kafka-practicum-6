# Образец результатов

Это иллюстрация ожидаемого формата, а не лог выполненного развёртывания.
ID схемы, RUN_ID, номера брокеров, offsets и порядок строк могут отличаться.

`curl http://localhost:8081/subjects`:

```json
["orders-value"]
```

`curl -X GET http://localhost:8081/subjects/orders-value/versions` после первой регистрации:

```json
[1]
```

Фрагмент `kafka-topics --describe --topic orders` (TopicId опущен):

```text
Topic: orders PartitionCount: 3 ReplicationFactor: 3 Configs: cleanup.policy=delete,retention.ms=86400000,segment.bytes=134217728,min.insync.replicas=2
Topic: orders Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3
Topic: orders Partition: 1 Leader: 2 Replicas: 2,3,1 Isr: 2,3,1
Topic: orders Partition: 2 Leader: 3 Replicas: 3,1,2 Isr: 3,1,2
```

Одна запись в логе продюсера:

```text
DELIVERED topic=orders partition=0 offset=0 value={"id": "example-0", "product": "book", "quantity": 1}
```

Та же запись в консьюмере:

```text
READ partition=0 offset=0 value={"id": "example-0", "product": "book", "quantity": 1}
```

В console-consumer виден JSON без служебного префикса:

```json
{"id": "example-0", "product": "book", "quantity": 1}
```

После записи в Hadoop:

```text
HDFS SAVED /events/orders/0/0.json id=example-0 OFFSET COMMITTED
```

`hdfs dfs -cat /events/orders/0/0.json` покажет тот же JSON.
Для каждого прогона должны присутствовать все шесть ID из лога продюсера.

Итог успешной автоматической проверки:

```text
PASS: schema registered; 3 partitions; RF=3; ISR=3; six records delivered, read and verified in HDFS.
```

В работающем локальном стенде `docker compose ps` показывает kafka-1, kafka-2,
kafka-3, schema-registry, namenode и datanode. В облачном режиме
локальных Kafka-контейнеров нет: брокерами управляет Yandex Cloud.
