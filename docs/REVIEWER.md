# Как проверить работу

В работе используется Hadoop HDFS. Ниже два варианта: локальная проверка всей цепочки
и запуск с Managed Kafka в своём каталоге Yandex Cloud. Платные ресурсы локальный режим не создаёт.

## 1. Подготовка

На Windows используйте Ubuntu в WSL2 и Docker Desktop с включённой интеграцией WSL.
Команды выполняются в Bash, из каталога репозитория. На Linux нужен Docker Engine
и плагин Compose v2. Для установки утилит в Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y git curl python3 coreutils
```

Docker устанавливается отдельно. Перед запуском `docker info` должен выполняться
без ошибки. Образы и Python-зависимости скачиваются из интернета при первом запуске.
Python-зависимости приложения устанавливаются внутри контейнера, вручную pip запускать не нужно.

```bash
git clone https://github.com/SVFr13nD/kafka-practicum-6.git
cd kafka-practicum-6
bash scripts/check-prerequisites.sh local
bash scripts/verify.sh local
cat evidence/local/result.txt
```

Проверка ожидает готовности Kafka, Registry и HDFS. Если готовность не наступила,
она завершается ошибкой. Успешный запуск заканчивается строкой `Verification passed`.
`result.txt` содержит PASS только после сравнения ID всех шести сообщений в продюсере,
консьюмере, console-consumer и HDFS, а также проверки партиций, репликации и ISR.

## 2. Посмотреть результаты самостоятельно

В новом терминале снова перейдите в каталог репозитория и задайте Compose-файлы:

```bash
export COMPOSE_FILE=docker-compose.yml:compose.local.yml
export CERTS_DIR=./.runtime/local
export CLIENT_ENV=config/client-local.env
export REGISTRY_ENV=config/registry-local.env
docker compose ps
curl -fsS http://localhost:8081/subjects; echo
curl -fsS http://localhost:8081/subjects/orders-value/versions; echo
docker compose run --rm -T cli '/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic orders'
cat evidence/local/producer.txt
cat evidence/local/consumer.txt
cat evidence/local/console-consumer.txt
cat evidence/local/hdfs-sink.txt
docker compose exec -T namenode hdfs dfs -ls -R /events/orders
docker compose exec -T namenode hdfs dfs -cat '/events/orders/*/*.json'
docker compose exec -T namenode hdfs fsck /events
```

Веб-интерфейс HDFS: http://localhost:9870. В меню Utilities → Browse the file system
откройте `/events/orders`. Порты доступны только на компьютере, где запущен стенд.

## 3. Отдельный запуск продюсера и консьюмера

После успешного первого запуска можно повторить обмен без пересоздания сервисов.
Все команды этого блока выполняются в одном терминале: RUN_ID должен совпадать.

```bash
export COMPOSE_FILE=docker-compose.yml:compose.local.yml
export CERTS_DIR=./.runtime/local
export CLIENT_ENV=config/client-local.env REGISTRY_ENV=config/registry-local.env
export RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
docker compose run --rm -T app python app/register.py
# Начальные offsets нужны, чтобы прочитать именно новую отправку.
export START_OFFSETS="$(docker compose run --rm -T cli '/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:9092 --topic orders --time -1' | sort -t: -k2,2n | awk -F: '{print $3}' | paste -sd, -)"
docker compose run --rm -T app python app/producer.py
docker compose run --rm -T app python app/consumer.py
docker compose run --rm -T app python app/hdfs_sink.py
```

Продюсер создаёт шесть JSON-документов по схеме `schemas/order.json` и распределяет
их по трём партициям. Пример набора — `examples/orders.jsonl`.
Файл служит иллюстрацией формата; продюсер генерирует данные сам.
В `app/producer.py` можно изменить `product` и `quantity`, сохранив ID текущего прогона.
Если `quantity` равен 0, проверка JSON Schema отклонит запись до отправки.

Для чтения первых шести записей топика стандартным CLI:

```bash
docker compose run --rm -T cli '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic orders --from-beginning --max-messages 6 --timeout-ms 60000'
```

При повторном запуске эта ручная команда может показать старые сообщения.
Автоматическая проверка читает точные offsets текущей отправки.
CLI находятся в `/opt/kafka/bin/` образа `apache/kafka:3.9.1`.

## 4. Запуск в Yandex Cloud

Полные команды Terraform и настройки VM приведены в разделе
[инструкции Terraform](../terraform/README.md).
Нужны свой аккаунт, каталог, квоты и платёжный аккаунт/грант. Шаблон настроек —
`terraform/terraform.tfvars.example`; заполните копию `terraform/terraform.tfvars`
либо используйте переменные `TF_VAR_*` из инструкции Terraform. Пример пароля в репозитории отсутствует.

На VM клонируйте репозиторий, создайте `.runtime`
командой `scripts/cloud-config.py` из инструкции Terraform, затем выполните:

```bash
sudo --preserve-env=BOOTSTRAP_SERVERS bash scripts/check-prerequisites.sh cloud
sudo --preserve-env=BOOTSTRAP_SERVERS bash scripts/verify.sh cloud
cat evidence/cloud/result.txt
```

В этом режиме Kafka работает в Managed Service, а Registry и Hadoop — на VM.
Проверка использует SASL_SSL. Настройки ресурсов, топиков и сети находятся в Terraform.
Команды просмотра результатов из раздела 2 применимы с `evidence/cloud/`;
для Docker задайте `COMPOSE_FILE=docker-compose.yml`,
`CLIENT_ENV=.runtime/client.env`, `REGISTRY_ENV=.runtime/registry.env`, `CERTS_DIR=./.runtime`.
При работе через sudo эти переменные также нужно передать через `--preserve-env`.

Список скриншотов для сдачи: [evidence/cloud/README.md](../evidence/cloud/README.md).
Скриншоты снимайте с фактического запуска. Примеры из `examples/` не являются подтверждением облачного развёртывания.

## 5. Остановка

Локально остановить сервисы и сохранить тома HDFS:

```bash
docker compose -f docker-compose.yml -f compose.local.yml stop
```

Полностью удалить локальный стенд **вместе с данными**:

```bash
docker compose -f docker-compose.yml -f compose.local.yml down -v
```

Облачный кластер не останавливается командой Docker. После копирования результатов
удалите созданный учебный стенд по инструкции Terraform:
 `terraform -chdir=terraform destroy` из каталога с исходным state.
Сохраните state до удаления инфраструктуры. Не оставляйте облачный стенд включённым после проверки.

## Если что-то не запустилось

| Симптом | Что проверить |
|---|---|
| `Missing command` | Установить указанную утилиту; запускать в Linux/WSL Bash |
| Cannot connect to Docker daemon | Запустить Docker; проверить `docker info` и права пользователя |
| Порт 8081 или 9870 занят | Остановить другой сервис, занимающий порт, и повторить запуск |
| Registry не готов | `docker compose logs schema-registry`; в облаке проверить FQDN, пароль, CA и доступ VM к 9091 |
| Нет Live datanodes | `docker compose logs namenode datanode`; проверить память и свободное место |
| Прочитано меньше шести сообщений | Проверить одинаковый RUN_ID и `producer.txt`, ошибки отправки, время ожидания |
| Terraform отклоняет ресурсы | Проверить квоты, доступность класса/версии Kafka и права в своём каталоге |

После ошибки смотрите `evidence/<режим>/docker.log` и `services.txt`.
Не отправляйте пароли, `.runtime`, Terraform state и `.tfvars` в репозиторий.

Без Docker можно проверить сохранённые результаты последнего прогона:

```bash
RUN_ID="$(python3 -c 'import json; from pathlib import Path; row=Path("evidence/local/producer.txt").read_text().split("value=",1)[1].splitlines()[0]; print(json.loads(row)["id"].rsplit("-",1)[0])')"
python3 scripts/check-results.py evidence/local "$RUN_ID"
```

Эта команда сверяет уже сохранённые файлы. Она не запускает сервисы.
