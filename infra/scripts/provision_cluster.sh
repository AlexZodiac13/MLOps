#!/bin/bash
set -e

LOG_FILE="log/terraform_provision.log"
SUMMARY_FILE="log/terraform_summary.txt"

# Очищаем логи перед началом
mkdir -p log
echo "" > $LOG_FILE
echo "" > $SUMMARY_FILE

echo "========================================"
echo "НАЧАЛО НАСТРОЙКИ КЛАСТЕРА"
echo "========================================"
echo "$(date): Получение информации о мастер-ноде..." | tee $LOG_FILE
echo "Логи очищены и готовы к записи" | tee -a $LOG_FILE

# Параметры из Terraform
YC_TOKEN=${YC_TOKEN}
YC_FOLDER_ID=${YC_FOLDER_ID}
PRIVATE_KEY_PATH=${PRIVATE_KEY_PATH}
ACCESS_KEY=${ACCESS_KEY}
SECRET_KEY=${SECRET_KEY}
S3_BUCKET=${S3_BUCKET}
ANSIBLE_ROOT=${ANSIBLE_ROOT}

# Get master node IP
MASTER_IP=$(yc --token $YC_TOKEN --folder-id $YC_FOLDER_ID compute instance list --format json | \
  jq -r '.[] | select(.labels.subcluster_role == "masternode") | .network_interfaces[0].primary_v4_address.one_to_one_nat.address')

MASTER_FQDN=$(yc --token $YC_TOKEN --folder-id $YC_FOLDER_ID compute instance list --format json | \
  jq -r '.[] | select(.labels.subcluster_role == "masternode") | .fqdn')

if [ -z "$MASTER_IP" ]; then
  echo "ERROR: Master node IP not found" | tee -a $LOG_FILE
  exit 1
fi

echo "Найдена мастер-нода: $MASTER_IP" | tee -a $LOG_FILE
echo "FQDN: $MASTER_FQDN" | tee -a $LOG_FILE

echo "Master IP: $MASTER_IP" > $SUMMARY_FILE
echo "Master FQDN: $MASTER_FQDN" >> $SUMMARY_FILE

echo "========================================"
echo "ЗАПУСК ANSIBLE PROVISIONING"
echo "========================================"
echo "$(date): Начинаем установку компонентов..." | tee -a $LOG_FILE
echo "INFO: Долгие операции (копирование данных) могут занять до 2 часов" | tee -a $LOG_FILE

# Run Ansible с агрессивными SSH keep-alive настройками
echo "Запуск Ansible playbook..." | tee -a $LOG_FILE
ANSIBLE_SSH_RETRIES=5 \
ANSIBLE_TIMEOUT=7200 \
ansible-playbook -i "$MASTER_IP," \
  --private-key $PRIVATE_KEY_PATH \
  -u ubuntu \
  --timeout=7200 \
  --ssh-common-args '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=30 -o ServerAliveCountMax=120 -o ConnectTimeout=60 -o TCPKeepAlive=yes -o Compression=yes' \
  --extra-vars "access_key_var=$ACCESS_KEY secret_key_var=$SECRET_KEY s3_bucket_var=$S3_BUCKET" \
  -v \
  $ANSIBLE_ROOT/ansible/master_playbook.yml 2>&1 | \
while IFS= read -r line; do
  timestamp=$(date +"%H:%M:%S")
  echo "$timestamp: $line" | tee -a $LOG_FILE
  
  # Прогресс-индикаторы для важных этапов
  case "$line" in
    *"Install packages"*) echo "$timestamp: [PROGRESS] Устанавливаем системные пакеты..." | tee -a $LOG_FILE ;;
    *"Sync ALL files"*) echo "$timestamp: [PROGRESS] Копируем ВСЕ файлы из S3 (долго, ~30-60 мин)..." | tee -a $LOG_FILE ;;
    *"Copy ALL files from S3 to HDFS"*) echo "$timestamp: [PROGRESS] Копируем в HDFS (долго, ~30-60 мин)..." | tee -a $LOG_FILE ;;
    *"Install Python packages"*) echo "$timestamp: [PROGRESS] Устанавливаем Python пакеты (может занять 5-10 мин)..." | tee -a $LOG_FILE ;;
    *"Downloading"*|*"Collecting"*) echo -n "." ;;
    *"Successfully installed"*) echo "$timestamp: [OK] Python пакеты установлены" | tee -a $LOG_FILE ;;
    *"Start Jupyter"*) echo "$timestamp: [PROGRESS] Запускаем Jupyter..." | tee -a $LOG_FILE ;;
    *"Jupyter Notebook запущен"*) echo "$timestamp: [OK] Jupyter запущен" | tee -a $LOG_FILE ;;
    *"Синхронизация всех файлов"*) echo "$timestamp: [OK] Синхронизация S3 завершена" | tee -a $LOG_FILE ;;
    *"Копирование в HDFS"*) echo "$timestamp: [OK] Копирование в HDFS завершено" | tee -a $LOG_FILE ;;
  esac
done

ANSIBLE_EXIT_CODE=$?

echo ""
echo "========================================"
echo "ЗАВЕРШЕНИЕ НАСТРОЙКИ"
echo "========================================"
echo "$(date): Ansible завершился с кодом: $ANSIBLE_EXIT_CODE" | tee -a $LOG_FILE

# Extract Jupyter token
JUPYTER_TOKEN=$(grep -o "Токен: [0-9a-f]*" $LOG_FILE | tail -1 | sed 's/Токен: //' || echo "NOT_FOUND")
if [ "$JUPYTER_TOKEN" = "NOT_FOUND" ]; then
  JUPYTER_TOKEN=$(grep -o "token=[0-9a-f]*" $LOG_FILE | tail -1 | sed 's/token=//' || echo "NOT_FOUND")
fi

# Extract public IP
PUBLIC_IP=$(grep -A5 "Display Jupyter access info" $LOG_FILE | grep -o "http://[0-9.]*:8888" | head -1 | sed 's|http://||;s|:8888||' || echo "$MASTER_IP")

# Заменяем запись токена и публичного IP на Jupyter URL и добавляем S3 креды
# (удаляем прежнюю запись токена line: echo "Jupyter Token: $JUPYTER_TOKEN" >> $SUMMARY_FILE)
# Убираем: echo "Public IP: $PUBLIC_IP" >> $SUMMARY_FILE
JUPYTER_URL="http://$PUBLIC_IP:8888/?token=$JUPYTER_TOKEN"
echo "Jupyter URL: $JUPYTER_URL" >> $SUMMARY_FILE

# Записываем S3 credentials в summary (по запросу)
echo "S3_ACCESS_KEY: $ACCESS_KEY" >> $SUMMARY_FILE
echo "S3_SECRET_KEY: $SECRET_KEY" >> $SUMMARY_FILE

# Print summary
echo ""
echo "========================================"
if [ $ANSIBLE_EXIT_CODE -eq 0 ]; then
    echo "[SUCCESS] PROVISIONING ЗАВЕРШЕНО УСПЕШНО"
else
    echo "[WARNING] PROVISIONING ЗАВЕРШЕНО С ПРЕДУПРЕЖДЕНИЯМИ (код: $ANSIBLE_EXIT_CODE)"
    echo "          Проверьте лог для деталей"
fi
echo "========================================"
cat $SUMMARY_FILE
echo "========================================"
echo ""
echo "Jupyter URL:"
echo "   http://$PUBLIC_IP:8888/?token=$JUPYTER_TOKEN"
echo ""
echo "   Или скопируйте из лога:"
grep -A3 "Внешний доступ:" $LOG_FILE | tail -1 | sed 's/^[^h]*//' || echo "   http://$PUBLIC_IP:8888/?token=$JUPYTER_TOKEN"
echo ""
echo "Данные в HDFS: /user/ubuntu/data/"
echo "Время завершения: $(date)"
echo "========================================"

# Возвращаем 0 даже при warning'ах, если основные задачи выполнены
if [ $ANSIBLE_EXIT_CODE -ne 0 ]; then
    if grep -q "Successfully installed" $LOG_FILE && grep -q "Jupyter Notebook запущен" $LOG_FILE; then
        echo "INFO: Основные компоненты установлены, несмотря на warning'и"
        exit 0
    fi
fi

exit $ANSIBLE_EXIT_CODE
