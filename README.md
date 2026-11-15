# Практикум 6: Kafka, SSL и права доступа

Учебный проект: три брокера Kafka, ZooKeeper и две небольшие программы на Python.
Producer отправляет сообщения, consumer их читает. Подключения клиентов и обмен
между брокерами защищены TLS (в настройках Kafka он называется SSL).

SSL шифрует соединение, сертификат определяет клиента, ACL задает его права.
Здесь используется **mTLS**: клиент и брокер проверяют сертификаты друг друга.

| Клиент | topic-1 | topic-2 |
| --- | --- | --- |
| producer | Может писать | Может писать |
| consumer | Может читать | Читать запрещено |
| admin | Управляет топиками и правами | Управляет топиками и правами |

Связь брокеров с ZooKeeper в этом примере без TLS, внутри Docker-сети.
Порт ZooKeeper на компьютер не опубликован. Это локальный стенд, не настройка
для продакшена. Пароль учебных хранилищ — `kafka123`.

## 1. Что установить

- Docker и Docker Compose.
- Python 3 с pip и venv.
- Git и Bash.
- **OpenSSL 3.2 или новее** с поддержкой `-jdktrust`.

Команды ниже для Bash: Linux, macOS или WSL на Windows.
На Windows Docker Desktop должен работать с Linux-контейнерами.
Проверяем инструменты:

```bash
docker compose version
python3 --version
openssl version
openssl pkcs12 -help 2>&1 | grep -- -jdktrust
```

Последняя команда должна показать опцию `-jdktrust`. Если ее нет,
обновите OpenSSL перед продолжением. Например, стандартного OpenSSL 3.0
недостаточно для этого скрипта.

```bash
git clone https://github.com/SVFr13nD/kafka-practicum-6.git
cd kafka-practicum-6
```

Все следующие команды выполняются из этой папки.

## 2. Создать сертификаты

```bash
bash scripts/generate-certs.sh
```

Появится папка `certs/` с центром сертификации (CA), сертификатами трех
брокеров и трех клиентов: `admin`, `producer`, `consumer`.

- `.crt` — сертификат.
- `.key` — приватный ключ.
- `keystore.p12` — ключ и сертификат для Java/Kafka.
- `truststore.p12` — доверенный CA для проверки другой стороны.

Скрипт не перезаписывает существующую папку `certs/`. При повторном обычном
запуске стенда генерацию пропускайте. Сертификаты действуют 365 дней.
Ключи и хранилища генерируются у вас и исключены из Git.
Для чтения контейнером учебные PKCS12 имеют права 644 и известный пароль;
не используйте их за пределами этого стенда.

## 3. Запустить Kafka

```bash
docker compose up -d
docker compose ps
```

Дождитесь запуска `zookeeper`, `kafka-1`, `kafka-2`, `kafka-3`.
Состояние `Up` еще не гарантирует, что Kafka готова. Проверьте API брокеров:

```bash
docker compose exec -T kafka-1 kafka-broker-api-versions \
  --bootstrap-server kafka-1:19093 \
  --command-config /etc/kafka/configs/admin.properties
```

В выводе должны быть доступны все три брокера (id 1, 2, 3).
Если они еще запускаются, подождите и повторите команду. При ошибках смотрите:

```bash
docker compose logs --tail=100 kafka-1 kafka-2 kafka-3
```

С компьютера Kafka доступна на `localhost:9093`, `localhost:9094`,
`localhost:9095`. Внутри Docker используются имена `kafka-1`, `kafka-2`,
`kafka-3` и порт `19093`. Не заменяйте `localhost` на произвольный IP:
адрес должен соответствовать сертификату и advertised listeners.

## 4. Создать топики и выдать права

```bash
docker compose exec -T kafka-1 bash /scripts/init-topics-acl.sh
```

Скрипт создает `topic-1` и `topic-2`: по три партиции и три копии данных.
Затем выдает права из таблицы выше. В конце печатает список ACL.
Сертификат `CN=producer` соответствует пользователю `User:CN=producer`,
аналогично для consumer и admin.

## 5. Установить Python-библиотеку

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r app/requirements.txt
```

Если в Ubuntu нет venv, установите системный пакет `python3-venv`, затем
повторите команды. В новом терминале окружение нужно активировать снова.

## 6. Отправить сообщения

```bash
python app/producer.py
```

Программа отправляет по три сообщения в каждый топик.
Ожидаемый результат — шесть строк `Отправлено в ...`.
Если отправка не подтверждена за отведенное время или получена ошибка,
программа завершится с кодом 1.

## 7. Проверить чтение

```bash
python app/consumer.py topic-1
```

При первом запуске группы появятся три сообщения. Consumer продолжает ждать
новые сообщения; остановить его можно через Ctrl+C.
Группа `consumer-group` сохраняет позицию чтения, поэтому при следующем
запуске старые сообщения могут не появиться. Просто запустите producer еще раз.

Теперь проверяем запрет:

```bash
python app/consumer.py topic-2
```

Ожидаемый результат: `Нет прав на чтение: topic-2`.
Это успешная проверка ACL, а не поломка SSL. Сообщения в этом топике есть,
но сертификату consumer запрещено их читать.

## 8. Проверить TLS отдельно

```bash
openssl s_client -connect localhost:9093 -verify_hostname localhost \
  -verify_return_error -CAfile certs/ca.crt \
  -cert certs/clients/producer/producer.crt \
  -key certs/clients/producer/producer.key </dev/null
```

Ищите `Verify return code: 0 (ok)` и отсутствие ошибок проверки.
Команда проверяет сертификат и TLS-соединение, но не права на топик:
права проверяем producer/consumer на предыдущих шагах.

## Если возникла ошибка

| Ошибка | Что проверить |
| --- | --- |
| unknown option: jdktrust | Нужен OpenSSL 3.2+ |
| Папка certs уже существует | Для обычного запуска используйте существующие сертификаты |
| Не найден сертификат | Выполнен ли шаг 2 до запуска контейнеров |
| Permission denied при чтении p12 | Скрипт задает права чтения PKCS12 для контейнеров; проверьте права и монтирование |
| SSL handshake failed | Все ли сертификаты выпущены одним CA, совпадают ли имя хоста и SAN |
| TopicAuthorizationFailed на topic-1 | Выполнен ли скрипт настройки ACL |
| Нет сообщений в topic-1 | Повторите запуск producer: группа могла уже прочитать старые сообщения |
| Replication factor larger than available brokers | Дождитесь готовности всех трех брокеров |
| Конфликт имени zookeeper/kafka-1 | Остановите другой учебный стенд с такими же именами контейнеров |
| Нет модуля confluent_kafka | Активируйте .venv и установите requirements |

## Остановить и продолжить

```bash
docker compose stop
docker compose start
```

`stop/start` оставляет те же контейнеры. Для полного сброса учебных данных:

```bash
docker compose down -v
docker compose up -d
```

**Сброс удаляет данные контейнеров и томов.** После него снова выполните
шаг 4. Папка `certs/` остается на компьютере. Постоянное хранение данных
для продакшена здесь не настроено.

## Файлы проекта

| Путь | Что внутри |
| --- | --- |
| docker-compose.yml | ZooKeeper и три брокера |
| configs/broker-common.env | Общие SSL и ACL настройки |
| configs/kafka-N.env | Адреса и хранилища каждого брокера |
| configs/*.properties | Настройки консольных Java-клиентов |
| scripts/generate-certs.sh | Выпуск сертификатов |
| scripts/init-topics-acl.sh | Топики и права доступа |
| app/ssl_config.py | Общие SSL-настройки Python |
| app/producer.py | Отправка шести сообщений |
| app/consumer.py | Чтение выбранного топика |

## Источник и изменения

Основа: [dyackonov777/kafka-practice_6_SSL](https://github.com/dyackonov777/kafka-practice_6_SSL),
версия `f3d5215f211324565ea656e1758ace3dd387bdc5`.
Сохранены три брокера, mTLS, проверка имени хоста и ACL.
Упрощены Python-программы, убраны YAML-якоря, инструкция переписана по шагам.
Готовые ключи исходного проекта не копировались; сертификаты создаются локально.
Порты брокеров опубликованы только на 127.0.0.1.

Справка: [mTLS в Confluent](https://docs.confluent.io/platform/current/security/authentication/mutual-tls/overview.html).

## Проверки перед публикацией

Проверены синтаксис Python/Bash/YAML, генерация сертификатов, цепочка доверия,
имена брокеров, чтение PKCS12 и обработка сообщений на тестовых подстановках.
Полный запуск Kafka в Docker здесь не выполнялся: Docker в среде подготовки
недоступен. Шаги 3–8 позволяют проверить стенд после скачивания.
