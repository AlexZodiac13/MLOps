"""
DAG для развёртывания инфраструктуры из каталога infra в S3 через Terraform.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago


import os
import json
import subprocess
import tempfile
from yandexcloud import SDK
import requests
import boto3
import xml.etree.ElementTree as ET
from yandex.cloud.compute.v1.instance_service_pb2 import ListInstancesRequest
from yandex.cloud.compute.v1.instance_service_pb2_grpc import InstanceServiceStub

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
    import zipfile
    import io
    import stat
    
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
    import zipfile
    import re
    
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

def install_dependencies(install_dir):
    """
    Installs dependencies: Ansible, YC CLI, JQ.
    Strictly uses local files from install_dir/files/.
    """
    import sys
    import shutil
    
    bin_dir = os.path.join(install_dir, 'bin')
    os.makedirs(bin_dir, exist_ok=True)
    
    # 1. Ansible
    # We cannot install Ansible from internet. 
    # We assume it is installed in the environment.
    print("Checking Ansible...")
    ansible_found = False
    try:
        subprocess.check_call([sys.executable, '-m', 'ansible', '--version'])
        ansible_found = True
    except Exception:
        pass
        
    if not ansible_found:
        # Try finding ansible-playbook in PATH or user base
        import site
        user_base = site.getuserbase()
        user_bin = os.path.join(user_base, 'bin')
        if os.path.exists(os.path.join(user_bin, 'ansible-playbook')):
            print(f"Found ansible in {user_bin}")
            # Symlink to bin_dir to ensure it's in PATH
            try:
                if not os.path.exists(os.path.join(bin_dir, 'ansible-playbook')):
                    os.symlink(os.path.join(user_bin, 'ansible-playbook'), os.path.join(bin_dir, 'ansible-playbook'))
                if not os.path.exists(os.path.join(bin_dir, 'ansible')):
                    os.symlink(os.path.join(user_bin, 'ansible'), os.path.join(bin_dir, 'ansible'))
            except OSError:
                pass
        else:
            print("Warning: Ansible not found. Provisioning might fail if not pre-installed.")

    # 2. Install YC CLI
    yc_path = os.path.join(bin_dir, 'yc')
    local_yc = os.path.join(install_dir, 'files', 'yc')
    if os.path.exists(local_yc):
        print(f"Found local yc at {local_yc}")
        shutil.copy(local_yc, yc_path)
        os.chmod(yc_path, 0o755)
    else:
        print("ERROR: 'yc' binary not found in files/yc.")

    # 3. Install JQ
    jq_path = os.path.join(bin_dir, 'jq')
    local_jq = os.path.join(install_dir, 'files', 'jq')
    if os.path.exists(local_jq):
        print(f"Found local jq at {local_jq}")
        shutil.copy(local_jq, jq_path)
        os.chmod(jq_path, 0o755)
    else:
        print("ERROR: 'jq' binary not found in files/jq.")

    if not os.path.exists(yc_path) or not os.path.exists(jq_path):
        raise Exception("Missing required dependencies (yc, jq). Please ensure they are in 'infra/files/' in S3.")
        
    return bin_dir

def run_terraform_apply(local_infra_dir, tf_binary, cli_config_path=None, backend_config=None, extra_path=None):
    env = os.environ.copy()
    if cli_config_path:
        env['TF_CLI_CONFIG_FILE'] = cli_config_path
        print(f"Using Terraform config file: {cli_config_path}")
    
    if extra_path:
        env['PATH'] = f"{extra_path}:{env.get('PATH', '')}"
        print(f"Added {extra_path} to PATH")
    
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

    print("Running terraform apply...")
    try:
        proc = subprocess.run(
            [tf_binary, "apply", "-auto-approve", "-input=false", "-no-color"],
            cwd=local_infra_dir,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        print(proc.stdout)
    except subprocess.CalledProcessError as e:
        print("Terraform apply failed:")
        print(e.stdout)
        print(e.stderr)
        raise

def get_tfvar(name, tfvars_dict, tfvars_file_path):
    if tfvars_dict and name in tfvars_dict:
        return tfvars_dict[name]
    
    # Try to read from file
    if os.path.exists(tfvars_file_path):
        with open(tfvars_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            import re
            # Regex for simple string assignment: key = "value"
            match = re.search(rf'^\s*{name}\s*=\s*"(.*)"', content, re.MULTILINE)
            if match:
                return match.group(1)
            
            # Regex for heredoc: key = <<EOF\n(.*?)\nEOF
            match = re.search(rf'^\s*{name}\s*=\s*<<([A-Z]+)\s*\n(.*?)\n\s*\1', content, re.MULTILINE | re.DOTALL)
            if match:
                return match.group(2)
    return None

def run_provisioning(tmpdir, tfvars, s3_bucket):
    tfvars_path = os.path.join(tmpdir, 'terraform.tfvars')
    
    # Try to get token from env first, then tfvars
    yc_token = os.environ.get('YC_TOKEN') or get_tfvar('yc_token', tfvars, tfvars_path)
    yc_folder_id = os.environ.get('YC_FOLDER_ID') or get_tfvar('yc_folder_id', tfvars, tfvars_path)
    private_key = get_tfvar('private_key', tfvars, tfvars_path)
    
    if not yc_token or not yc_folder_id:
        print("Skipping provisioning: yc_token or yc_folder_id not found")
        return

    # We rely on the shell script to find the Master IP using 'yc' CLI.
    # This avoids double authentication issues with Python SDK.
    
    env = os.environ.copy()
    env['YC_TOKEN'] = yc_token
    env['YC_FOLDER_ID'] = yc_folder_id
    env['PRIVATE_KEY_CONTENT'] = private_key if private_key else ""
    # env['MASTER_IP'] = ... # Script will find it
    env['S3_BUCKET'] = s3_bucket
    env['ANSIBLE_ROOT'] = os.path.join(tmpdir, 'ansible')
    env['ACCESS_KEY'] = Variable.get("AWS_ACCESS_KEY_ID")
    env['SECRET_KEY'] = Variable.get("AWS_SECRET_ACCESS_KEY")

    script_path = os.path.join(tmpdir, 'scripts', 'provision_cluster.sh')
    os.chmod(script_path, 0o755)
    
    print("Running provisioning script...")
    # Ensure bin_dir is in PATH for the script to find 'yc' and 'jq'
    bin_dir = os.path.join(tmpdir, 'bin')
    env['PATH'] = f"{bin_dir}:{env.get('PATH', '')}"
    
    try:
        proc = subprocess.run(
            ["bash", script_path],
            cwd=tmpdir,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        print(proc.stdout)
    except subprocess.CalledProcessError as e:
        print("Provisioning failed:")
        print(e.stdout)
        print(e.stderr)
        raise

def upload_summary_to_s3(local_dir, s3_bucket, s3_prefix):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name='ru-central1'
    )

    # Upload summary
    summary_file = os.path.join(local_dir, 'log', 'terraform_summary.txt')
    if os.path.exists(summary_file):
        s3_key = f"{s3_prefix}log/terraform_summary.txt"
        print(f"Uploading summary file to s3://{s3_bucket}/{s3_key}")
        s3.upload_file(summary_file, s3_bucket, s3_key)
    else:
        print(f"Summary file not found at {summary_file}")

    # Upload provision log
    provision_log = os.path.join(local_dir, 'log', 'terraform_provision.log')
    if os.path.exists(provision_log):
        s3_key = f"{s3_prefix}log/terraform_provision.log"
        print(f"Uploading provision log to s3://{s3_bucket}/{s3_key}")
        s3.upload_file(provision_log, s3_bucket, s3_key)
    else:
        print(f"Provision log not found at {provision_log}")

def deploy_infra(**context):
    # Try to get bucket from Variable, fallback to params
    s3_bucket = Variable.get("airflow-bucket-name", default_var=context['params'].get('s3_bucket'))
    print(f"Using S3 bucket: {s3_bucket}")
    
    s3_prefix = context['params']['infra_prefix']
    tfvars = context['params']['tfvars']
    run_id = context['run_id'].replace(':', '_').replace('.', '_')
    tmpdir = f"/tmp/infra_{run_id}"
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
    
    print("Installing dependencies (Ansible, YC, JQ)...")
    bin_dir = install_dependencies(tmpdir)

    backend_config = {
        'bucket': s3_bucket,
        'access_key': Variable.get("AWS_ACCESS_KEY_ID"),
        'secret_key': Variable.get("AWS_SECRET_ACCESS_KEY")
    }

    print("Running Terraform Apply...")
    run_terraform_apply(tmpdir, tf_binary, cli_config_path, backend_config, extra_path=bin_dir)
    
    print("Running Cluster Provisioning...")
    run_provisioning(tmpdir, tfvars, s3_bucket)
    
    print("Uploading summary to S3...")
    upload_summary_to_s3(tmpdir, s3_bucket, s3_prefix)

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'deploy_infra',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)


from airflow.operators.bash import BashOperator

deploy_task = PythonOperator(
    task_id='deploy_infra_task',
    python_callable=deploy_infra,
    provide_context=True,
    params={
        's3_bucket': 'airflow-bucket-name',
        'infra_prefix': 'infra/',
        'tfvars': {
            # Здесь перечислите необходимые переменные для terraform
            # 'key': 'value',
        }
    },
    dag=dag)

