# Практическая работа 7. Kafka и Hadoop

Продюсер отправляет шесть заказов в топик `orders`, по два в каждую партицию. Один консьюмер читает их в терминал, второй записывает в HDFS. Схема заказа хранится в Schema Registry под именем `orders-value`. В сообщении передаётся обычный JSON, поэтому его можно прочитать через `kafka-console-consumer.sh`.

Проверка выполнена в Docker Compose на GitHub Actions. В Yandex Cloud ресурсы не создавались. [Результат проверки](evidence/local/result.txt) · [Запуск и версия кода](evidence/local/run.txt)

## Проверяющему

Скопируйте репозиторий и запустите проверку одной командой:

```bash
git clone https://github.com/SVFr13nD/kafka-practicum-6.git
cd kafka-practicum-6
bash scripts/verify.sh local
cat evidence/local/result.txt
```

Нужны Linux или Ubuntu в WSL2, работающий Docker с Compose v2, Python 3, curl, git,
coreutils (команда timeout), интернет и около 8 ГБ памяти для Docker.

[Пошаговая инструкция](docs/REVIEWER.md) включает ручную проверку, повторную отправку,
запуск в Yandex Cloud, остановку и разбор ошибок.
[Шесть примеров сообщений](examples/orders.jsonl) · [Примеры ответов и логов](examples/expected-output.md).
Примеры показывают формат; реальные результаты предыдущего локального запуска приведены ниже.
Аккаунт Yandex Cloud для локального режима не требуется.

## Что проверено

| Что посмотреть | Результат |
|---|---|
| Схема и партиции | [Терминал с двумя запросами curl и описанием топика](evidence/local/terminal-registry.png) · [полный вывод kafka-topics.sh](evidence/local/topic-describe.txt) |
| Отправка и чтение | [Терминал с логами продюсера и консьюмера](evidence/local/terminal-clients.png) · [все записи kafka-console-consumer.sh](evidence/local/console-consumer.txt) |
| Запись в Hadoop | [Запущенные сервисы и список файлов в HDFS](evidence/local/terminal-hadoop.png) · [данные из файлов](evidence/local/hdfs-data.jsonl) · [проверка HDFS](evidence/local/hdfs-fsck.txt) |
| Состояние кластера | [Кворум KRaft](evidence/local/quorum.txt) · [интерфейс NameNode](evidence/local/hdfs-ui.png) |

У `orders` три партиции и три реплики. В `topic-describe.txt` у каждой партиции указаны лидер, три реплики и три синхронные реплики (ISR). Продюсер ждёт подтверждения от всех текущих ISR (`acks=all`). Если их остаётся меньше двух, брокер отклоняет запись (`min.insync.replicas=2`).

| Топик | Партиции | Реплики | Параметры хранения |
|---|---:|---:|---|
| `orders` | 3 | 3 | `cleanup.policy=delete`, `retention.ms=86400000` (24 часа), `segment.bytes=134217728` (128 МиБ) |
| `orders-schemas` | 1 | 3 | `cleanup.policy=compact`, по ключу сохраняется актуальная запись реестра |

HDFS-консьюмер использует путь `/events/orders/<partition>/<offset>.json` и подтверждает offset после записи. При повторной доставке он перезаписывает тот же файл. Проверка сверяет все поля заказов с выводом обоих консьюмеров и с содержимым файлов в HDFS.

## Запуск в Docker

Понадобятся Docker Compose v2, Bash, Python 3 и curl. Для стенда достаточно выделить Docker около 8 ГБ памяти.

```bash
bash scripts/verify.sh local
curl http://localhost:8081/subjects
curl -X GET http://localhost:8081/subjects/orders-value/versions
```

Вывод сохраняется в `evidence/local/`. Скрипт фиксирует начальные offsets перед отправкой, поэтому его можно запускать повторно на том же стенде. Чтобы остановить контейнеры с сохранением данных, выполни `docker compose -f docker-compose.yml -f compose.local.yml down`.

## Конфигурация Yandex Cloud

По умолчанию Terraform описывает три брокера в зоне `kz1-a`. На брокер выделены 2 vCPU, 8 ГБ RAM и один сетевой SSD на 32 ГБ. Те же три узла образуют кворум KRaft. Репликация выдерживает потерю одного брокера, но все три зависят от одной зоны. Для Schema Registry и одноузлового HDFS предусмотрена отдельная ВМ с 4 vCPU, 8 ГБ RAM и SSD на 40 ГБ. Клиенты подключаются к приватным адресам брокеров по `SASL_SSL` с `SCRAM-SHA-512`.

[Terraform и порядок запуска](terraform/README.md) · [Схема заказа](schemas/order.json) · [Продюсер](app/producer.py) · [Консьюмер](app/consumer.py) · [Запись в HDFS](app/hdfs_sink.py)
