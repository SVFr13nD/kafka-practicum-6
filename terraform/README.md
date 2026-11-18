# Параметры облачного стенда

Профиль `kz` рассчитан на три брокера Apache Kafka 3.9 в зоне `kz1-a`. В профиле `ru` создаётся по одному брокеру в зонах `ru-central1-a`, `ru-central1-b` и `ru-central1-d`. В обоих профилях используется совмещённый режим KRaft. У Kafka нет публичных адресов. SSH на ВМ разрешён с адреса `admin_cidr`.

`main.tf` задаёт брокерам `log.retention.ms=86400000` и `log.segment.bytes=134217728`. У `orders` политика очистки `delete`, у служебного топика `orders-schemas` — `compact`. Такой топик можно создать через API Yandex Cloud. Настройки Hadoop и Schema Registry лежат в корне репозитория.

В GitHub Actions выполняется только проверка конфигурации Terraform. Команды ниже относятся к отдельному запуску в каталоге с доступными облачными ресурсами. `apply` создаёт тарифицируемые Kafka и ВМ.

## Создание стенда

Команды выполняются из корня репозитория. Для Казахстана выбери настроенный профиль `yc` региона `kz`. Понадобятся Terraform 1.9 или новее и SSH-ключ для доступа к ВМ.

```bash
export TF_VAR_region=kz
export TF_VAR_folder_id="$(yc config get folder-id)"
export YC_TOKEN="$(yc iam create-token)"
export TF_VAR_ssh_public_key="$(cat ~/.ssh/id_ed25519.pub)"
read -rp 'Внешний IPv4 для SSH (без /32): ' ip
export TF_VAR_admin_cidr="$ip/32"
export TF_VAR_password="Aa1_$(openssl rand -hex 18)"

terraform -chdir=terraform init
terraform -chdir=terraform plan
terraform -chdir=terraform apply
```

## Проверка

В той же оболочке запиши адреса брокеров и ВМ из выходных параметров. Пароль Kafka передаётся на ВМ по SSH через стандартный ввод.

```bash
vm="ubuntu@$(terraform -chdir=terraform output -raw vm_ip)"
brokers="$(terraform -chdir=terraform output -raw bootstrap_servers)"

ssh "$vm" 'sudo cloud-init status --wait && git clone --branch reviewer-ready https://github.com/SVFr13nD/kafka-practicum-6.git'
printf '%s\n' "$brokers" "$TF_VAR_password" "$TF_VAR_region" | ssh "$vm" '
  set -eu
  read -r BOOTSTRAP_SERVERS
  read -r KAFKA_PASSWORD
  read -r YC_REGION
  export BOOTSTRAP_SERVERS KAFKA_PASSWORD YC_REGION
  cd kafka-practicum-6
  python3 scripts/cloud-config.py
  unset KAFKA_PASSWORD
  sudo --preserve-env=BOOTSTRAP_SERVERS bash scripts/verify.sh cloud
'
scp -r "$vm:kafka-practicum-6/evidence/cloud/." evidence/cloud/
```

При повторной проверке пропусти `git clone`. Для доступа к Registry и веб-интерфейсу HDFS можно открыть туннель `ssh -N -L 8081:localhost:8081 -L 9870:localhost:9870 "$vm"`. Логи продюсера, консьюмера и HDFS сохраняются на ВМ в `evidence/cloud/`.

## Удаление

После сохранения результатов команда `terraform -chdir=terraform destroy` удаляет ресурсы из state этой работы, включая диски ВМ и Kafka. До завершения удаления ресурсы могут тарифицироваться. Пароль пользователя Kafka содержится в Terraform state. State и `.runtime/` исключены из Git.

[Документация по созданию кластера](https://yandex.cloud/ru-kz/docs/managed-kafka/operations/cluster-create) · [Подключение клиентов и сертификат](https://yandex.cloud/ru-kz/docs/managed-kafka/operations/connect/) · [Региональные настройки Terraform](https://yandex.cloud/ru-kz/docs/terraform/quickstart)
