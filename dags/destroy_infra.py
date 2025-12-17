"""
DAG для удаления инфраструктуры из каталога infra в S3 через Terraform.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

import os
import json
import subprocess
import tempfile
import boto3
import zipfile
import stat
import re

def download_infra_from_s3(s3_bucket, s3_prefix, local_dir):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name='ru-central1'
    )
    
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            key = obj['Key']
            if key.endswith('/'):
                continue
            # Skip state files as they are handled by the backend
            if key.endswith('.tfstate') or key.endswith('.tfstate.backup'):
                continue
            rel_path = os.path.relpath(key, s3_prefix)
            local_path = os.path.join(local_dir, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(s3_bucket, key, local_path)

def install_terraform(install_dir):
    # 1. Try to install from local files (if downloaded from S3 as part of infra)
    local_zip_path = os.path.join(install_dir, 'files', 'terraform_1.5.7_linux_amd64.zip')
    if os.path.exists(local_zip_path):
        print(f"Found local terraform zip at {local_zip_path}")
        with zipfile.ZipFile(local_zip_path, 'r') as z:
            z.extractall(install_dir)
        tf_path = os.path.join(install_dir, 'terraform')
        st = os.stat(tf_path)
        os.chmod(tf_path, st.st_mode | stat.S_IEXEC)
        return tf_path

    # Try to download from S3 first (if user uploaded it)
    try:
        s3_bucket = Variable.get("airflow-bucket-name", default_var=None)
        if s3_bucket:
            session = boto3.session.Session()
            s3 = session.client(
                service_name='s3',
                endpoint_url='https://storage.yandexcloud.net',
                aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
                region_name='ru-central1'
            )
            tf_path = os.path.join(install_dir, 'terraform')
            # Try to download 'terraform' binary directly or 'terraform.zip'
            try:
                s3.download_file(s3_bucket, 'terraform', tf_path)
                st = os.stat(tf_path)
                os.chmod(tf_path, st.st_mode | stat.S_IEXEC)
                print("Downloaded terraform binary from S3")
                return tf_path
            except Exception:
                pass
    except Exception as e:
        print(f"Could not download from S3: {e}")

    raise Exception("Terraform binary not found in local files or S3. Internet download is disabled.")

def setup_local_providers(install_dir):
    """
    Sets up a local filesystem mirror for Terraform providers.
    Expects provider zips in install_dir/files/
    """
    plugins_dir = os.path.join(install_dir, 'plugins_mirror')
    os.makedirs(plugins_dir, exist_ok=True)
    
    # Define known providers patterns
    known_providers = [
        {
            'pattern': r'terraform-provider-yandex_([\d\.]+)_linux_amd64.zip',
            'namespace': 'yandex-cloud',
            'type': 'yandex'
        },
        {
            'pattern': r'terraform-provider-null_([\d\.]+)_linux_amd64.zip',
            'namespace': 'hashicorp',
            'type': 'null'
        }
    ]
    
    files_dir = os.path.join(install_dir, 'files')
    if not os.path.exists(files_dir):
        return None

    has_providers = False
    available_files = os.listdir(files_dir)
    
    for p in known_providers:
        for filename in available_files:
            match = re.match(p['pattern'], filename)
            if match:
                version = match.group(1)
                zip_path = os.path.join(files_dir, filename)
                target_dir = os.path.join(plugins_dir, 'registry.terraform.io', p['namespace'], p['type'], version, 'linux_amd64')
                os.makedirs(target_dir, exist_ok=True)
                print(f"Extracting {filename} to {target_dir}")
                with zipfile.ZipFile(zip_path, 'r') as z:
                    z.extractall(target_dir)
                
                # Make executable
                for root, dirs, files in os.walk(target_dir):
                    for f in files:
                        os.chmod(os.path.join(root, f), 0o755)
                has_providers = True
            
    if has_providers:
        # Create .terraformrc
        rc_path = os.path.join(install_dir, '.terraformrc')
        with open(rc_path, 'w') as f:
            f.write(f"""
provider_installation {{
  filesystem_mirror {{
    path    = "{plugins_dir}"
    include = ["registry.terraform.io/*/*"]
  }}
  direct {{
    exclude = ["registry.terraform.io/*/*"]
  }}
}}
""")
        print(f"Created .terraformrc at {rc_path}")
        return rc_path
    return None

def run_terraform_destroy(local_infra_dir, tf_binary, cli_config_path=None, backend_config=None):
    env = os.environ.copy()
    if cli_config_path:
        env['TF_CLI_CONFIG_FILE'] = cli_config_path
        print(f"Using Terraform config file: {cli_config_path}")
    
    init_cmd = [tf_binary, "init", "-input=false", "-no-color"]
    if backend_config:
        for k, v in backend_config.items():
            init_cmd.append(f"-backend-config={k}={v}")

    print(f"Running terraform init...")
    try:
        proc = subprocess.run(
            init_cmd,
            cwd=local_infra_dir,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        print(proc.stdout)
    except subprocess.CalledProcessError as e:
        print("Terraform init failed:")
        print(e.stdout)
        print(e.stderr)
        raise

    print("Running terraform destroy...")
    try:
        proc = subprocess.run(
            [tf_binary, "destroy", "-auto-approve", "-input=false", "-no-color"],
            cwd=local_infra_dir,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        print(proc.stdout)
    except subprocess.CalledProcessError as e:
        print("Terraform destroy failed:")
        print(e.stdout)
        print(e.stderr)
        raise

def destroy_infra(**context):
    # Try to get bucket from Variable, fallback to params
    s3_bucket = Variable.get("airflow-bucket-name", default_var=context['params'].get('s3_bucket'))
    print(f"Using S3 bucket: {s3_bucket}")

    s3_prefix = context['params']['infra_prefix']
    tfvars = context['params']['tfvars']
    run_id = context['run_id'].replace(':', '_').replace('.', '_')
    tmpdir = f"/tmp/infra_destroy_{run_id}"
    os.makedirs(tmpdir, exist_ok=True)
    
    print(f"Downloading infra from {s3_bucket}/{s3_prefix} to {tmpdir}")
    download_infra_from_s3(s3_bucket, s3_prefix, tmpdir)
    
    # Сохраняем tfvars в файл (дописываем, если есть параметры)
    tfvars_path = os.path.join(tmpdir, 'terraform.tfvars')
    if tfvars:
        with open(tfvars_path, 'a', encoding='utf-8') as f:
            f.write("\n# Variables from Airflow params\n")
            for k, v in tfvars.items():
                if isinstance(v, str):
                    f.write(f'{k} = "{v}"\n')
                else:
                    f.write(f'{k} = {json.dumps(v, ensure_ascii=False)}\n')

    print("Installing Terraform...")
    tf_binary = install_terraform(tmpdir)
    
    print("Setting up local providers...")
    cli_config_path = setup_local_providers(tmpdir)
    
    backend_config = {
        'bucket': s3_bucket,
        'access_key': Variable.get("AWS_ACCESS_KEY_ID"),
        'secret_key': Variable.get("AWS_SECRET_ACCESS_KEY")
    }

    print("Running Terraform Destroy...")
    run_terraform_destroy(tmpdir, tf_binary, cli_config_path, backend_config)

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'destroy_infra',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

destroy_task = PythonOperator(
    task_id='destroy_infra',
    python_callable=destroy_infra,
    provide_context=True,
    params={
        's3_bucket': 'airflow-bucket-name',
        'infra_prefix': 'infra/',
        'tfvars': {
            # Здесь перечислите необходимые переменные для terraform
            # 'key': 'value',
        }
    },
    dag=dag,
)
