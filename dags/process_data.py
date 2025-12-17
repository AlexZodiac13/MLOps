"""
DAG для обработки данных на кластере Spark.
1. Получает IP и FQDN мастер-ноды из S3 (terraform_summary.txt).
2. Подготавливает скрипт очистки данных (подставляет переменные).
3. Загружает скрипт на мастер-ноду по SSH.
4. Запускает spark-submit.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import boto3
import paramiko
import os
import time
import io

def get_cluster_info(**context):
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
    
    try:
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
    except Exception as e:
        print(f"Error getting cluster info: {e}")
        raise

def get_private_key():
    # 1. Try Variable
    try:
        key = Variable.get("SSH_PRIVATE_KEY")
        return key
    except:
        pass
        
    # 2. Try local tfvars file (fallback for development)
    # Assuming DAG is in dags/ and tfvars in airflow/terraform.tfvars or infra/terraform.tfvars
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tfvars_paths = [
        os.path.join(base_dir, 'airflow', 'terraform.tfvars'),
        os.path.join(base_dir, 'infra', 'terraform.tfvars')
    ]
    
    for path in tfvars_paths:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
                import re
                match = re.search(r'private_key\s*=\s*<<EOT\n(.*?)\nEOT', content, re.DOTALL)
                if match:
                    return match.group(1)
    
    raise Exception("SSH Private Key not found in Variables (SSH_PRIVATE_KEY) or local tfvars files.")

def run_spark_job(**context):
    ti = context['task_instance']
    cluster_info = ti.xcom_pull(task_ids='get_cluster_info')
    master_ip = cluster_info['master_ip']
    master_fqdn = cluster_info['master_fqdn']
    
    s3_bucket = Variable.get("airflow-bucket-name")
    
    # Paths
    hdfs_input_dir = "/user/ubuntu/data"
    hdfs_output_dir = "/user/ubuntu/clean/transactions_parquet"
    s3_output_path = f"s3a://{s3_bucket}/clean/transactions_parquet"
    
    # Read script template
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script_path = os.path.join(base_dir, 'infra', 'scripts', 'clear_dataset.py')
    
    with open(script_path, 'r', encoding='utf-8') as f:
        script_content = f.read()
        
    # Substitute variables
    script_content = script_content.replace('{HDFS_NAMENODE}', master_fqdn)
    script_content = script_content.replace('{HDFS_INPUT_DIR}', hdfs_input_dir)
    script_content = script_content.replace('{HDFS_OUTPUT_DIR}', hdfs_output_dir)
    script_content = script_content.replace('{S3_OUTPUT_PATH}', s3_output_path)
    script_content = script_content.replace('{S3_ACCESS_KEY}', Variable.get("AWS_ACCESS_KEY_ID"))
    script_content = script_content.replace('{S3_SECRET_KEY}', Variable.get("AWS_SECRET_ACCESS_KEY"))
    
    # SSH Connection
    key_str = get_private_key()
    
    # Paramiko is picky about key format. It needs the key to be stripped of extra whitespace
    # but keep the internal newlines.
    key_str = key_str.strip()
    
    # If the key doesn't have the header/footer, we might need to add it, 
    # but usually it comes with it from tfvars.
    # Let's ensure it's treated as a file object correctly.
    key_file_obj = io.StringIO(key_str)
    
    try:
        # Try loading as RSA key (most common for generated keys)
        pkey = paramiko.RSAKey.from_private_key(key_file_obj)
    except Exception as e:
        print(f"Failed to load key as RSA: {e}. Trying Ed25519...")
        # Reset file pointer
        key_file_obj.seek(0)
        try:
             # Try Ed25519 (newer keys)
            pkey = paramiko.Ed25519Key.from_private_key(key_file_obj)
        except Exception as e2:
            print(f"Failed to load key as Ed25519: {e2}")
            raise Exception(f"Could not load private key. Ensure it is a valid OpenSSH private key. Error: {e}")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print(f"Connecting to {master_ip}...")
    ssh.connect(master_ip, username='ubuntu', pkey=pkey)
    
    # Upload script
    remote_script_path = "/home/ubuntu/clean_transactions.py"
    sftp = ssh.open_sftp()
    with sftp.file(remote_script_path, 'w') as f:
        f.write(script_content)
    sftp.close()
    print(f"Uploaded script to {remote_script_path}")
    
    # Run spark-submit
    # Using nohup or just running it. Since we want to wait for completion, we run directly.
    # We need to ensure environment variables are set if needed, but usually spark-submit is in path or we use full path.
    # In the provided script comment: spark-submit /home/ubuntu/clean_transactions.py
    
    cmd = f"spark-submit {remote_script_path}"
    print(f"Executing: {cmd}")
    
    stdin, stdout, stderr = ssh.exec_command(cmd)
    
    # Stream output
    exit_status = stdout.channel.recv_exit_status()
    
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    
    print("STDOUT:", out)
    print("STDERR:", err)
    
    ssh.close()
    
    if exit_status != 0:
        raise Exception(f"Spark job failed with exit code {exit_status}")
    
    print("Spark job completed successfully.")

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'process_data',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

get_info_task = PythonOperator(
    task_id='get_cluster_info',
    python_callable=get_cluster_info,
    dag=dag
)

run_spark_task = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    provide_context=True,
    dag=dag
)

get_info_task >> run_spark_task
