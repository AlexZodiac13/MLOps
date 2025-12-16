**Цель**: настроить и запустить Managed Airflow в Yandex Cloud через Terraform.

Коротко:
- Проверьте и заполните `terraform.tfvars` (см. `terraform.tfvars.example`).
- Убедитесь, что в папке `airflow` заданы корректные `network_id` и `subnet_ids`.
  - По умолчанию конфигурация может создавать VPC и подсеть автоматически (`create_network = true`). Если вы хотите использовать существующую подсеть, установите `create_network = false` и укажите `subnet_ids` (resource id, не CIDR).
  - `subnet_ids` должны быть Resource ID подсетей (например, `b1g...`) — не указывайте CIDR напрямую. Если вы указали CIDR, получите id подсети через `yc` CLI или используйте скрипт `scripts/resolve_subnet_id.sh`.
- Прочитайте официальную документацию по ресурсу:
  https://yandex.cloud/ru/docs/terraform/resources/airflow

Важные шаги перед запуском:
- Сгенерируйте/получите `yc_token` (OAuth) и выставьте в `terraform.tfvars` или
  как переменную окружения `TF_VAR_yc_token`.
- Укажите `admin_password` в `terraform.tfvars` (строка `admin_password = "<your-admin-password>"`).
- Проверьте совместимость `airflow_version` и `python_version` (в `terraform.tfvars`) — Managed Airflow обычно требует полного номера Python (включая патч). Если API возвращает ошибку несовместимости, выполните либо:
  - Запрос через `yc` CLI (см. helper-скрипт `scripts/list_supported_airflow_python_versions.sh`), либо
  - Попробуйте одну из распространённых версий: `3.11.7`, `3.10.13`, `3.9.18`, `3.8.17`.
  По умолчанию сейчас стоит `python_version = "3.11.7"`.
- Убедитесь, что у вашей учётной записи достаточно прав для создания MDB и
  IAM-ресурсов (roles/owner или эквивалентные права).
- Проверьте политику доступа к бакету S3/Object Storage: управляемый Airflow должен иметь доступ на чтение/запись к `bucket_name`. Если вы создаёте сервисный аккаунт через Terraform, я добавил `yandex_storage_bucket_grant` для привязки прав.
 - Чтобы DAG'и и содержимое `infra/` автоматически были доступны в Object Storage (и следовательно у Airflow после `code_sync`), файлы загружаются Terraform'ом при `upload_with_terraform = true`.
  - Опция `upload_with_terraform` (в `airflow/variables.tf`) по умолчанию `true` — создаёт `yandex_storage_object` для файлов и sentinel `mlops/.upload_complete`.
  - DAG'и теперь автоматически загружают `infra/` из S3 в значение `infra_path` перед выполнением `terraform` (для этого нужны Airflow Variables `s3_bucket` и `infra_path`).

  ВНИМАНИЕ: большие/численные файлы могут замедлить `terraform plan`/`apply`. Можно сузить набор загружаемых путей при необходимости.
- Если при создании кластера вы получили ошибку про запись метрик или `integrationProvider`, назначьте сервисному аккаунту роль `managed-airflow.integrationProvider`. По умолчанию роль добавляется, если `create_airflow_service_account = true`. Если у вашей учётной записи нет прав на назначение ролей, назначьте роль вручную через `yc` CLI:

  ```bash
  # замените на ваш folder-id и service account id (в outputs):
  yc resource-manager folder add-access-binding \
    --folder-id <your-folder-id> \
    --role managed-airflow.integrationProvider \
    --subject serviceAccount:<service-account-id>
  ```


Как применить (локально, Windows PowerShell):

```powershell
cd airflow
./apply.ps1 # или запустите явные команды ниже
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

Проверки и отладка:
- Если `terraform plan` сообщает об отсутствии полей в ресурсе Airflow, сверяйтесь с документацией и обновляйте `main.tf` согласно актуальной схеме.
- После `apply` проверьте outputs (например, URL веб-интерфейса) и проверьте логи
  в Yandex Cloud Console.

Если хотите, могу добавить примеры привязки IAM (service account → bucket)
и автоматическое создание service account — скажите, нужно ли это сделать.
