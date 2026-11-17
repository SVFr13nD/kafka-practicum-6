# Подтверждения запуска в Yandex Cloud

Статус: облачный запуск не выполнен в этой сессии. Ниже чек-лист, а не результаты.

После `bash scripts/verify.sh cloud` проверьте `result.txt` и сохраните реальные скриншоты:

| Файл скриншота | Что показать |
|---|---|
| 01-cluster.png | Консоль Yandex Cloud: кластер, три брокера, зоны, CPU/RAM/диски |
| 02-registry.png | Терминал VM: `curl http://localhost:8081/subjects` и `curl -X GET http://localhost:8081/subjects/orders-value/versions` |
| 03-topic.png | `topic-describe.txt`: 3 партиции, RF=3, ISR |
| 04-transfer.png | `producer.txt`, `consumer.txt` и `console-consumer.txt` текущего прогона |
| 05-services.png | `docker compose ps`, запущенные Registry, NameNode, DataNode |
| 06-hdfs.png | `hdfs-sink.txt`, `hdfs-files.txt`, `hdfs-data.jsonl`, `hdfs-fsck.txt` |

Используйте снимки своего терминала/консоли, не подставные изображения из примеров.
В `run.txt` зафиксированы режим, RUN_ID и commit. Дополнительно запишите дату и ID кластера.
Перед сдачей проверьте отсутствие паролей, токенов и персональных данных.
Файлы здесь исключены из git: добавляйте просмотренные результаты явно через `git add -f`.
После копирования результатов остановите кластер и VM по README; для удаления всех
созданных ресурсов используйте `terraform destroy`, предварительно сохранив нужные данные.
