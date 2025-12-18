"""
Combined DAG: Deploy Infra -> Process Data -> Destroy Infra
Runs in a loop with a delay.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import json
import subprocess
import boto3
import zipfile
import stat
import re
import sys
import shutil
import paramiko
import io
import time

# --- Shared Helper Functions ---

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
            if key.endswith('.tfstate') or key.endswith('.tfstate.backup'):
                continue
            rel_path = os.path.relpath(key, s3_prefix)
            local_path = os.path.join(local_dir, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(s3_bucket, key, local_path)

def install_terraform(install_dir):
    local_zip_path = os.path.join(install_dir, 'files', 'terraform_1.5.7_linux_amd64.zip')
    if os.path.exists(local_zip_path):
        print(f"Found local terraform zip at {local_zip_path}")
        with zipfile.ZipFile(local_zip_path, 'r') as z:
            z.extractall(install_dir)
        tf_path = os.path.join(install_dir, 'terraform')
        st = os.stat(tf_path)
        os.chmod(tf_path, st.st_mode | stat.S_IEXEC)
        return tf_path

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

    raise Exception("Terraform binary not found in local files or S3.")

def setup_local_providers(install_dir):
    plugins_dir = os.path.join(install_dir, 'plugins_mirror')
    os.makedirs(plugins_dir, exist_ok=True)
    
    known_providers = [
        {'pattern': r'terraform-provider-yandex_([\d\.]+)_linux_amd64.zip', 'namespace': 'yandex-cloud', 'type': 'yandex'},
        {'pattern': r'terraform-provider-null_([\d\.]+)_linux_amd64.zip', 'namespace': 'hashicorp', 'type': 'null'}
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
                for root, dirs, files in os.walk(target_dir):
                    for f in files:
                        os.chmod(os.path.join(root, f), 0o755)
                has_providers = True
            
    if has_providers:
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
    import sys
    import shutil
    
    bin_dir = os.path.join(install_dir, 'bin')
    os.makedirs(bin_dir, exist_ok=True)
    
    print("Checking Ansible...")
    ansible_found = False
    
    python_bin_dir = os.path.dirname(sys.executable)
    ansible_playbook_path = os.path.join(python_bin_dir, 'ansible-playbook')
    ansible_path = os.path.join(python_bin_dir, 'ansible')
    
    if os.path.exists(ansible_playbook_path):
        print(f"Found ansible-playbook at {ansible_playbook_path}")
        try:
            if not os.path.exists(os.path.join(bin_dir, 'ansible-playbook')):
                os.symlink(ansible_playbook_path, os.path.join(bin_dir, 'ansible-playbook'))
            if os.path.exists(ansible_path) and not os.path.exists(os.path.join(bin_dir, 'ansible')):
                os.symlink(ansible_path, os.path.join(bin_dir, 'ansible'))
            ansible_found = True
        except OSError:
            pass

    if not ansible_found:
        try:
            subprocess.check_call([sys.executable, '-m', 'ansible', '--version'])
        except Exception:
            pass
        
        import site
        try:
            user_base = site.getuserbase()
            user_bin = os.path.join(user_base, 'bin')
            if os.path.exists(os.path.join(user_bin, 'ansible-playbook')):
                print(f"Found ansible in {user_bin}")
                try:
                    if not os.path.exists(os.path.join(bin_dir, 'ansible-playbook')):
                        os.symlink(os.path.join(user_bin, 'ansible-playbook'), os.path.join(bin_dir, 'ansible-playbook'))
                    if not os.path.exists(os.path.join(bin_dir, 'ansible')):
                        os.symlink(os.path.join(user_bin, 'ansible'), os.path.join(bin_dir, 'ansible'))
                    ansible_found = True
                except OSError:
                    pass
        except Exception:
            pass

    if not ansible_found:
        print("Ansible not found. Attempting to install ansible-core via pip into a venv...")
        try:
            venv_dir = os.path.join(install_dir, 'venv')
            import venv
            venv.create(venv_dir, with_pip=True)
            
            venv_python = os.path.join(venv_dir, 'bin', 'python3')
            if not os.path.exists(venv_python):
                 venv_python = os.path.join(venv_dir, 'bin', 'python')

            subprocess.check_call([
                venv_python, '-m', 'pip', 'install', 
                'ansible-core', 
                '--no-cache-dir'
            ])
            
            ansible_bin_dir = os.path.join(venv_dir, 'bin')
            if os.path.exists(os.path.join(ansible_bin_dir, 'ansible-playbook')):
                print(f"Successfully installed ansible-core to {venv_dir}")
                for binary in ['ansible', 'ansible-playbook']:
                    src = os.path.join(ansible_bin_dir, binary)
                    dst = os.path.join(bin_dir, binary)
                    if os.path.exists(src):
                        if os.path.exists(dst):
                            os.remove(dst)
                        os.symlink(src, dst)
                        os.chmod(src, 0o755)
                ansible_found = True
            else:
                print("Installed ansible-core but binaries not found in expected location.")
                
        except Exception as e:
            print(f"Failed to install ansible-core: {e}")

    if not ansible_found:
        print("Warning: Ansible binary (ansible-playbook) not found. Provisioning might fail.")

    yc_path = os.path.join(bin_dir, 'yc')
    local_yc = os.path.join(install_dir, 'files', 'yc')
    if os.path.exists(local_yc):
        shutil.copy(local_yc, yc_path)
        os.chmod(yc_path, 0o755)
    else:
        print("ERROR: 'yc' binary not found in files/yc.")

    jq_path = os.path.join(bin_dir, 'jq')
    local_jq = os.path.join(install_dir, 'files', 'jq')
    if os.path.exists(local_jq):
        shutil.copy(local_jq, jq_path)
        os.chmod(jq_path, 0o755)
    else:
        print("ERROR: 'jq' binary not found in files/jq.")

    if not os.path.exists(yc_path) or not os.path.exists(jq_path):
        raise Exception("Missing required dependencies (yc, jq).")
        
    return bin_dir

def run_terraform_apply(local_infra_dir, tf_binary, cli_config_path=None, backend_config=None, extra_path=None):
    env = os.environ.copy()
    if cli_config_path:
        env['TF_CLI_CONFIG_FILE'] = cli_config_path
    
    if extra_path:
        env['PATH'] = f"{extra_path}:{env.get('PATH', '')}"
        
    if 'PYTHONPATH' in os.environ:
        env['PYTHONPATH'] = os.environ['PYTHONPATH']
    
    init_cmd = [tf_binary, "init", "-input=false", "-no-color"]
    if backend_config:
        for k, v in backend_config.items():
            init_cmd.append(f"-backend-config={k}={v}")

    print(f"Running terraform init...")
    subprocess.run(init_cmd, cwd=local_infra_dir, env=env, capture_output=True, text=True, check=True)

    print("Running terraform apply...")
    subprocess.run([tf_binary, "apply", "-auto-approve", "-input=false", "-no-color"], cwd=local_infra_dir, env=env, capture_output=True, text=True, check=True)

def run_terraform_destroy(local_infra_dir, tf_binary, cli_config_path=None, backend_config=None):
    env = os.environ.copy()
    if cli_config_path:
        env['TF_CLI_CONFIG_FILE'] = cli_config_path
    
    init_cmd = [tf_binary, "init", "-input=false", "-no-color"]
    if backend_config:
        for k, v in backend_config.items():
            init_cmd.append(f"-backend-config={k}={v}")

    print(f"Running terraform init...")
    subprocess.run(init_cmd, cwd=local_infra_dir, env=env, capture_output=True, text=True, check=True)

    print("Running terraform destroy...")
    subprocess.run([tf_binary, "destroy", "-auto-approve", "-input=false", "-no-color"], cwd=local_infra_dir, env=env, capture_output=True, text=True, check=True)

def get_tfvar(name, tfvars_dict, tfvars_file_path):
    if tfvars_dict and name in tfvars_dict:
        return tfvars_dict[name]
    if os.path.exists(tfvars_file_path):
        with open(tfvars_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            match = re.search(rf'^\s*{name}\s*=\s*"(.*)"', content, re.MULTILINE)
            if match: return match.group(1)
            match = re.search(rf'^\s*{name}\s*=\s*<<([A-Z]+)\s*\n(.*?)\n\s*\1', content, re.MULTILINE | re.DOTALL)
            if match: return match.group(2)
    return None

def run_provisioning(tmpdir, tfvars, s3_bucket):
    tfvars_path = os.path.join(tmpdir, 'terraform.tfvars')
    yc_token = os.environ.get('YC_TOKEN') or get_tfvar('yc_token', tfvars, tfvars_path)
    yc_folder_id = os.environ.get('YC_FOLDER_ID') or get_tfvar('yc_folder_id', tfvars, tfvars_path)
    private_key = get_tfvar('private_key', tfvars, tfvars_path)
    
    if not yc_token or not yc_folder_id:
        print("Skipping provisioning: yc_token or yc_folder_id not found")
        return

    env = os.environ.copy()
    env['YC_TOKEN'] = yc_token
    env['YC_FOLDER_ID'] = yc_folder_id
    env['PRIVATE_KEY_CONTENT'] = private_key if private_key else ""
    env['S3_BUCKET'] = s3_bucket
    env['ANSIBLE_ROOT'] = os.path.join(tmpdir, 'ansible')
    env['ACCESS_KEY'] = Variable.get("AWS_ACCESS_KEY_ID")
    env['SECRET_KEY'] = Variable.get("AWS_SECRET_ACCESS_KEY")

    script_path = os.path.join(tmpdir, 'scripts', 'provision_cluster.sh')
    os.chmod(script_path, 0o755)
    
    bin_dir = os.path.join(tmpdir, 'bin')
    env['PATH'] = f"{bin_dir}:{env.get('PATH', '')}"
    
    print("Running provisioning script...")
    subprocess.run(["bash", script_path], cwd=tmpdir, env=env, capture_output=True, text=True, check=True)

def upload_summary_to_s3(local_dir, s3_bucket, s3_prefix):
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name='ru-central1'
    )
    for fname in ['terraform_summary.txt', 'terraform_provision.log']:
        fpath = os.path.join(local_dir, 'log', fname)
        if os.path.exists(fpath):
            s3.upload_file(fpath, s3_bucket, f"{s3_prefix}log/{fname}")

def get_private_key():
    try:
        return Variable.get("SSH_PRIVATE_KEY")
    except:
        pass
    # Fallback logic omitted for brevity in combined DAG, assuming Variable is set or passed via tfvars
    # But we can try to read from Variables loaded from JSON
    val = Variable.get("private_key", default_var=None)
    if val: return val
    raise Exception("SSH Private Key not found in Variables")

# --- Task Callables ---

def deploy_infra_op(**context):
    s3_bucket = Variable.get("airflow-bucket-name", default_var=context['params'].get('s3_bucket'))
    s3_prefix = context['params'].get('infra_prefix', 'infra/')
    
    tfvars = {}
    vars_to_map = [
        "yc_token", "yc_cloud_id", "yc_folder_id", "yc_zone",
        "yc_subnet_name", "yc_service_account_name", "yc_network_name",
        "yc_route_table_name", "yc_nat_gateway_name", "yc_security_group_name",
        "yc_subnet_range", "yc_dataproc_cluster_name", "yc_dataproc_version",
        "public_key", "private_key", "dataproc_master_resources",
        "dataproc_data_resources", "dataproc_data_hosts_count"
    ]
    for var_name in vars_to_map:
        val = Variable.get(var_name, default_var=None)
        if val is not None:
            try:
                if val.strip().startswith('{') or val.strip().startswith('['):
                     tfvars[var_name] = json.loads(val)
                else:
                     tfvars[var_name] = val
            except Exception:
                tfvars[var_name] = val

    if 'tfvars' in context['params']:
        tfvars.update(context['params']['tfvars'])

    run_id = context['run_id'].replace(':', '_').replace('.', '_')
    tmpdir = f"/tmp/infra_{run_id}"
    os.makedirs(tmpdir, exist_ok=True)
    
    print(f"Downloading infra from {s3_bucket}/{s3_prefix} to {tmpdir}")
    download_infra_from_s3(s3_bucket, s3_prefix, tmpdir)
    
    tfvars_path = os.path.join(tmpdir, 'terraform.tfvars')
    if tfvars:
        with open(tfvars_path, 'a', encoding='utf-8') as f:
            f.write("\n# Variables from Airflow params and Variables\n")
            for k, v in tfvars.items():
                f.write(f'{k} = {json.dumps(v, ensure_ascii=False)}\n')

    print("Installing Terraform...")
    tf_binary = install_terraform(tmpdir)
    print("Setting up local providers...")
    cli_config_path = setup_local_providers(tmpdir)
    print("Installing dependencies...")
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
    
    # Pass tmpdir to next tasks via XCom if needed, but we use S3 for state
    return tmpdir

def get_cluster_info_op(**context):
    s3_bucket = Variable.get("airflow-bucket-name")
    s3_prefix = "infra/"
    
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name='ru-central1'
    )
    
    key = f"{s3_prefix}log/terraform_summary.txt"
    print(f"Downloading {key} from {s3_bucket}...")
    
    response = s3.get_object(Bucket=s3_bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    print("Summary content:", content)
    
    info = {}
    for line in content.splitlines():
        if "Master IP:" in line:
            info['master_ip'] = line.split("Master IP:")[1].strip()
        if "Master FQDN:" in line:
            info['master_fqdn'] = line.split("Master FQDN:")[1].strip()
    
    if 'master_ip' not in info or 'master_fqdn' not in info:
        raise Exception("Could not find Master IP or Master FQDN in summary file")
        
    return info

def run_spark_job_op(**context):
    ti = context['task_instance']
    cluster_info = ti.xcom_pull(task_ids='get_cluster_info')
    master_ip = cluster_info['master_ip']
    master_fqdn = cluster_info['master_fqdn']
    
    s3_bucket = Variable.get("airflow-bucket-name")
    
    hdfs_input_dir = "/user/ubuntu/data"
    hdfs_output_dir = "/user/ubuntu/clean/transactions_parquet"
    s3_output_path = f"s3a://{s3_bucket}/clean/transactions_parquet"
    
    # We need to get the script content. In the original DAG it read from local file.
    # Here we can download it from S3 (since we uploaded infra there) or assume it's in the tmpdir of deploy task?
    # But deploy task tmpdir might be gone or on another worker.
    # Best is to download from S3 again.
    
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name='ru-central1'
    )
    
    script_key = "infra/scripts/clear_dataset.py"
    print(f"Downloading script {script_key}...")
    obj = s3.get_object(Bucket=s3_bucket, Key=script_key)
    script_content = obj['Body'].read().decode('utf-8')

    # Substitute variables
    script_content = script_content.replace('{HDFS_NAMENODE}', master_fqdn)
    script_content = script_content.replace('{HDFS_INPUT_DIR}', hdfs_input_dir)
    script_content = script_content.replace('{HDFS_OUTPUT_DIR}', hdfs_output_dir)
    script_content = script_content.replace('{S3_OUTPUT_PATH}', s3_output_path)
    script_content = script_content.replace('{S3_ACCESS_KEY}', Variable.get("AWS_ACCESS_KEY_ID"))
    script_content = script_content.replace('{S3_SECRET_KEY}', Variable.get("AWS_SECRET_ACCESS_KEY"))
    
    key_str = get_private_key().strip()
    key_file_obj = io.StringIO(key_str)
    
    try:
        pkey = paramiko.RSAKey.from_private_key(key_file_obj)
    except Exception:
        key_file_obj.seek(0)
        pkey = paramiko.Ed25519Key.from_private_key(key_file_obj)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print(f"Connecting to {master_ip}...")
    ssh.connect(master_ip, username='ubuntu', pkey=pkey)
    
    remote_script_path = "/home/ubuntu/clean_transactions.py"
    sftp = ssh.open_sftp()
    with sftp.file(remote_script_path, 'w') as f:
        f.write(script_content)
    sftp.close()
    
    cmd = f"spark-submit {remote_script_path}"
    print(f"Executing: {cmd}")
    
    stdin, stdout, stderr = ssh.exec_command(cmd)
    exit_status = stdout.channel.recv_exit_status()
    
    print("STDOUT:", stdout.read().decode().strip())
    print("STDERR:", stderr.read().decode().strip())
    
    ssh.close()
    
    if exit_status != 0:
        raise Exception(f"Spark job failed with exit code {exit_status}")

def destroy_infra_op(**context):
    s3_bucket = Variable.get("airflow-bucket-name", default_var=context['params'].get('s3_bucket'))
    s3_prefix = context['params'].get('infra_prefix', 'infra/')
    
    tfvars = {}
    vars_to_map = [
        "yc_token", "yc_cloud_id", "yc_folder_id", "yc_zone",
        "yc_subnet_name", "yc_service_account_name", "yc_network_name",
        "yc_route_table_name", "yc_nat_gateway_name", "yc_security_group_name",
        "yc_subnet_range", "yc_dataproc_cluster_name", "yc_dataproc_version",
        "public_key", "private_key", "dataproc_master_resources",
        "dataproc_data_resources", "dataproc_data_hosts_count"
    ]
    for var_name in vars_to_map:
        val = Variable.get(var_name, default_var=None)
        if val is not None:
            try:
                if val.strip().startswith('{') or val.strip().startswith('['):
                     tfvars[var_name] = json.loads(val)
                else:
                     tfvars[var_name] = val
            except Exception:
                tfvars[var_name] = val

    if 'tfvars' in context['params']:
        tfvars.update(context['params']['tfvars'])

    run_id = context['run_id'].replace(':', '_').replace('.', '_')
    tmpdir = f"/tmp/infra_destroy_{run_id}"
    os.makedirs(tmpdir, exist_ok=True)
    
    print(f"Downloading infra from {s3_bucket}/{s3_prefix} to {tmpdir}")
    download_infra_from_s3(s3_bucket, s3_prefix, tmpdir)
    
    tfvars_path = os.path.join(tmpdir, 'terraform.tfvars')
    if tfvars:
        with open(tfvars_path, 'a', encoding='utf-8') as f:
            f.write("\n# Variables from Airflow params and Variables\n")
            for k, v in tfvars.items():
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

# --- DAG Definition ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'end_to_end_cycle',
    default_args=default_args,
    description='Deploy -> Process -> Destroy cycle',
    schedule_interval=timedelta(minutes=30), # Run every 30 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={
        's3_bucket': 'airflow-bucket-name', # Will be overridden by Variable if not passed
        'infra_prefix': 'infra/',
        'tfvars': {}
    }
)

t1 = PythonOperator(
    task_id='deploy_infra',
    python_callable=deploy_infra_op,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id='get_cluster_info',
    python_callable=get_cluster_info_op,
    provide_context=True,
    dag=dag
)

t3 = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job_op,
    provide_context=True,
    dag=dag
)

t4 = PythonOperator(
    task_id='destroy_infra',
    python_callable=destroy_infra_op,
    provide_context=True,
    dag=dag
)

t1 >> t2 >> t3 >> t4
