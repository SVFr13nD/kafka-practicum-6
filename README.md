# Практическая работа 7 — Kafka и Hadoop

Репозиторий сохранён под именем `kafka-practicum-6`, работа внутри — №7.

**Статус:** подготовлена конфигурация Yandex Cloud и исполняемый стенд Kafka → Hadoop HDFS. Облачное развёртывание пока не выполнено: вход в консоль Yandex Cloud заблокирован проверкой автоматических запросов. Результаты `evidence/local/` относятся только к запуску в GitHub Actions. Они не заменяют требуемые подтверждения запуска в Yandex Cloud.

## Схема работы

Продюсер → топик `orders` → консьюмер → Hadoop HDFS `/events/orders/<partition>/<offset>.json`.

Schema Registry хранит JSON Schema `orders-value` в `_schemas`. Продюсер и консьюмер получают зарегистрированную схему через REST и проверяют документы с помощью jsonschema. В Kafka лежит обычный UTF-8 JSON (без Confluent binary framing), поэтому его можно читать стандартным `kafka-console-consumer.sh`.

## Ресурсы и параметры

| Компонент | Запланированная конфигурация Yandex Cloud |
|---|---|
| Kafka | 3 брокера: по одному в ru-central1-a, b, d |
| Каждый брокер | s3-c2-m8: 2 vCPU, 8 ГБ RAM, один сетевой SSD 32 ГБ |
| ZooKeeper | Управляется сервисом; класс s3-c2-m8, SSD 10 ГБ на хост |
| VM интеграции | 4 vCPU, 8 ГБ RAM, загрузочный SSD 40 ГБ |
| VM сервисы | Confluent Schema Registry, Hadoop NameNode и DataNode, клиенты |
| Соединение с Kafka | Приватная сеть, SASL_SSL, SCRAM-SHA-512, проверка CA и имени сервера |

Это отправная точка для небольшой учебной нагрузки, не результат production-бенчмарка. Увеличивать диск нужно по фактическому объёму входящих данных, сроку хранения и запасу места. При RF=3 полезная ёмкость существенно меньше суммарных 96 ГБ. В реальном production HDFS нужны отдельные DataNode, репликация ≥3, HA NameNode, Kerberos и мониторинг. Здесь HDFS одноузловой, репликация 1; его порты не доступны из интернета.

`brokers_count = 1` в Yandex означает **один брокер на зону**, итого три, а не один.

| Топик | Разделы | Реплики | Очистка | Хранение | Сегмент | min ISR |
|---|---:|---:|---|---|---|---:|
| orders | 3 | 3 | delete | 86400000 мс, 24 часа | 134217728 байт, 128 МиБ | 2 |
| _schemas | 1 | 3 | compact | Реестр схем | По умолчанию | 2 |

На уровне топика используются имена `cleanup.policy`, `retention.ms`, `segment.bytes`; соответствующие broker defaults называются `log.cleanup.policy`, `log.retention.ms`, `log.segment.bytes`. Topic overrides задаются в Terraform. Продюсер использует `acks=all`. Удаление старых сообщений выполняется по закрытым сегментам, а не строго в момент истечения 24 часов.

## Задание 1: запуск в Yandex Cloud

Предварительно нужны активный аккаунт, платёжный аккаунт/грант, каталог с квотами, настроенные `yc`, Terraform >=1.9, SSH-ключ, Docker Compose v2. Сам грант код не активирует. Не публиковать IAM-токены, пароли, `.tfvars`, state и `.runtime`.

```bash
git clone https://github.com/SVFr13nD/kafka-practicum-6.git
cd kafka-practicum-6
export YC_TOKEN="$(yc iam create-token)"
export TF_VAR_folder_id="$(yc config get folder-id)"
export TF_VAR_ssh_public_key="$(cat ~/.ssh/id_ed25519.pub)"
export TF_VAR_admin_cidr='ВАШ_ВНЕШНИЙ_IP/32'
export TF_VAR_password="$(openssl rand -hex 20)"
terraform -chdir=terraform init
terraform -chdir=terraform plan
terraform -chdir=terraform apply
terraform -chdir=terraform output
```

Terraform создаёт отдельную сеть, три подсети, security group, Managed Kafka, топики, пользователя и VM. Интернет-доступ к Kafka выключен. SSH разрешён только с указанного `/32`; Registry и HDFS UI опубликованы лишь на localhost VM. Cloud-init устанавливает Docker, Compose и git; его завершение проверить командой `sudo cloud-init status --wait`.

На созданной VM (подключение `ssh ubuntu@<vm_ip>`):

```bash
sudo cloud-init status --wait
git clone https://github.com/SVFr13nD/kafka-practicum-6.git
cd kafka-practicum-6
# FQDN взять из terraform output bootstrap_servers.
export BOOTSTRAP_SERVERS='broker1:9091,broker2:9091,broker3:9091'
read -rsp 'Пароль Kafka (тот же TF_VAR_password): ' KAFKA_PASSWORD; echo
export KAFKA_PASSWORD
# Установка requests из изолированного контейнера, не в системный Python.
sudo --preserve-env=BOOTSTRAP_SERVERS,KAFKA_PASSWORD docker run --rm -e BOOTSTRAP_SERVERS -e KAFKA_PASSWORD \
  -v "$PWD:/work" -w /work python:3.12-slim \
  sh -c 'pip install requests==2.32.3 && python scripts/cloud-config.py'
sudo chown -R "$USER:$USER" .runtime
sudo --preserve-env=BOOTSTRAP_SERVERS bash scripts/verify.sh cloud
```

При использовании `sudo docker run` передать переменные через `sudo --preserve-env=BOOTSTRAP_SERVERS,KAFKA_PASSWORD docker run ...`, иначе sudo может удалить их. Проверить наличие переменных до запуска.

Проверка выполняет регистрацию схемы, получение subjects/versions, describe топика, отправку шести сообщений, чтение и сохранение в HDFS. В образе Confluent стандартные CLI называются `kafka-topics` и `kafka-console-consumer` без суффикса `.sh`.

Требуемые вызовы на VM:

```bash
curl http://localhost:8081/subjects
curl -X GET http://localhost:8081/subjects/orders-value/versions
```

Для просмотра Registry/HDFS с компьютера использовать SSH-туннель:

```bash
ssh -L 8081:localhost:8081 -L 9870:localhost:9870 ubuntu@<vm_ip>
```

## Задание 2: Hadoop HDFS

Запускается тем же скриптом, отдельная ручная настройка не нужна. XML-настройки находятся в `hadoop/`, запуск — в `docker-compose.yml`, код интеграции — `app/hdfs_sink.py`.

После записи файла WebHDFS консьюмер синхронно фиксирует Kafka offset. При сбое до commit сообщение может быть получено повторно: путь из topic/partition/offset и overwrite предотвращают появление второго файла для того же Kafka-сообщения. Это at-least-once обработка с идемпотентной записью; транзакционной exactly-once гарантии между Kafka и HDFS нет. Формат с одним файлом на сообщение выбран для наглядности; production требует пакетной записи больших файлов.

Проверяются содержимое HDFS, соответствие идентификаторов отправленным сообщениям, наличие трёх ISR у каждой партиции и результат `hdfs fsck`.

## Проверка без облака

```bash
bash scripts/verify.sh local
```

Нужны Docker Compose v2, Python 3, curl и около 8 ГБ RAM для Docker. Этот режим поднимает три Kafka-брокера, ZooKeeper, Registry и HDFS в Docker. Kafka PLAINTEXT доступна только внутри Docker-сети. Это функциональная проверка кода, а не выполнение требования об облачной среде.

Скрипт рассчитан на чистый тестовый стенд. Для повторного локального прогона с нуля:

```bash
docker compose -f docker-compose.yml -f compose.local.yml down -v
bash scripts/verify.sh local
```

## Подтверждения

Автоматический запуск: вкладка [Actions](../../actions). Логи `evidence/local/` появляются только после успешной проверки. `run.txt` содержит URL запуска и SHA проверенного кода.

После облачного запуска `evidence/cloud/` должен содержать:

- `subjects.json`, `versions.json` — реальные ответы Registry;
- `topic-describe.txt`, `brokers.txt` — настройки и брокеры;
- `producer.txt`, `consumer.txt`, `console-consumer.txt` — обмен сообщениями;
- `services.txt`, `hdfs-sink.txt`, `hdfs-files.txt`, `hdfs-data.jsonl`, `hdfs-fsck.txt` — сервисы и сохранённые данные;
- скриншоты консоли с ответами curl, логами клиентов, работающими сервисами и HDFS;
- снимок Managed Kafka в Yandex Cloud с тремя брокерами и конфигурацией ресурсов.

Облачные evidence исключены из git по умолчанию: перед публикацией проверить, что там нет паролей/токенов. Публиковать только просмотренные файлы явно через `git add -f <путь>`.

## Завершение и расходы

После сбора результатов остановить Kafka и VM:

```bash
yc managed-kafka cluster stop "$(terraform -chdir=terraform output -raw cluster_id)"
yc compute instance stop practicum7-integration
```

Остановка не удаляет диски и может оставлять платные ресурсы. Чтобы полностью убрать созданную инфраструктуру после сохранения evidence:

```bash
terraform -chdir=terraform destroy
```

Проверить отсутствие ресурсов в консоли и начисления в Billing. Не оставлять кластер включённым после практической работы и не рассчитывать, что окончание гранта само остановит расходы.
