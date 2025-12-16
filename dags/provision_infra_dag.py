from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Настройки: переменные Airflow (установите в UI -> Admin -> Variables)
INFRA_PATH = Variable.get('infra_path', default_var='/opt/airflow/infra')
S3_BUCKET = Variable.get('s3_bucket', default_var='')
YC_TOKEN = Variable.get('yc_token', default_var='')
YC_FOLDER_ID = Variable.get('yc_folder_id', default_var='')
PRIVATE_KEY_PATH = Variable.get('private_key_path', default_var='')
ACCESS_KEY = Variable.get('s3_access_key', default_var='')
SECRET_KEY = Variable.get('s3_secret_key', default_var='')
ANSIBLE_ROOT = Variable.get('ansible_root', default_var=f'{INFRA_PATH}/ansible')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

with DAG(
    'provision_infra',
    default_args=default_args,
    description='Run terraform apply and then Ansible provisioning from infra/',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['infra'],
) as dag:

    fetch_cmd = (
        "mkdir -p {dest} && "
        "if command -v aws >/dev/null 2>&1; then "
        "  aws s3 sync s3://{bucket}/infra {dest} --endpoint-url https://storage.yandexcloud.net; "
        "elif command -v yc >/dev/null 2>&1 && command -v jq >/dev/null 2>&1; then "
        "  yc storage object list --bucket-name {bucket} --format json | jq -r '.[] | select(.name|startswith(\"infra/\")) | .name' | while read -r name; do mkdir -p \"{dest}/$(dirname \"${{name#infra/}}\")\"; yc storage object download --bucket-name {bucket} --name \"$name\" --file \"{dest}/${{name#infra/}}\"; done; "
        "else echo 'No aws or yc+jq available' >&2; exit 2; fi"
    ).format(bucket=S3_BUCKET, dest=INFRA_PATH)

    fetch_infra = BashOperator(
        task_id='fetch_infra',
        bash_command=fetch_cmd,
        env={
            'YC_TOKEN': YC_TOKEN,
            'YC_FOLDER_ID': YC_FOLDER_ID,
        },
    )

    terraform_init = BashOperator(
        task_id='terraform_init',
        bash_command=f'cd {INFRA_PATH} && terraform init -input=false',
        env={
            'YC_TOKEN': YC_TOKEN,
            'YC_FOLDER_ID': YC_FOLDER_ID,
        },
    )

    # Note: sync task removed — files are uploaded via Terraform (`upload_with_terraform=true`).

    terraform_apply = BashOperator(
        task_id='terraform_apply',
        bash_command=f'cd {INFRA_PATH} && terraform apply -auto-approve -var-file=terraform.tfvars',
        env={
            'YC_TOKEN': YC_TOKEN,
            'YC_FOLDER_ID': YC_FOLDER_ID,
        },
    )

    ansible_provision = BashOperator(
        task_id='ansible_provision',
        bash_command=f'{INFRA_PATH}/scripts/provision_cluster.sh',
        env={
            'YC_TOKEN': YC_TOKEN,
            'YC_FOLDER_ID': YC_FOLDER_ID,
            'PRIVATE_KEY_PATH': PRIVATE_KEY_PATH,
            'ACCESS_KEY': ACCESS_KEY,
            'SECRET_KEY': SECRET_KEY,
            'S3_BUCKET': S3_BUCKET,
            'ANSIBLE_ROOT': ANSIBLE_ROOT,
        },
    )

    fetch_infra >> terraform_init >> terraform_apply >> ansible_provision

# NOTE: Ensure Airflow Variables exist: infra_path, s3_bucket, yc_token, yc_folder_id, s3_access_key, s3_secret_key
