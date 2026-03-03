import yaml
import os

filepath = "/home/zodiac/stadygit/MLOps/infra/k8s/03-airflow.yaml"
with open(filepath, "r") as f:
    docs = list(yaml.safe_load_all(f))

# Define the volumes we want to add for Airflow parts
volumes = [
    {"name": "airflow-dags", "configMap": {"name": "airflow-dags-cm"}},
    {"name": "airflow-logs", "emptyDir": {}},  # For temporary logs across containers or you can use PVC
    {"name": "airflow-plugins", "emptyDir": {}},
]
volume_mounts = [
    {"name": "airflow-dags", "mountPath": "/opt/airflow/dags"},
    {"name": "airflow-logs", "mountPath": "/opt/airflow/logs"},
    {"name": "airflow-plugins", "mountPath": "/opt/airflow/plugins"},
]

for doc in docs:
    if doc and doc.get("kind") in ["Deployment", "Job"]:
        # Only add to airflow webserver/scheduler/worker/init
        if "airflow" in doc["metadata"]["name"]:
            spec = doc["spec"]["template"]["spec"]
            # Add volumes
            if "volumes" not in spec:
                spec["volumes"] = []
            
            existing_vols = [v["name"] for v in spec["volumes"]]
            for vol in volumes:
                if vol["name"] not in existing_vols:
                    spec["volumes"].append(vol)

            # Add volume mounts to the first container
            container = spec["containers"][0]
            if "volumeMounts" not in container:
                container["volumeMounts"] = []
            
            existing_mounts = [m["name"] for v in container["volumeMounts"]]
            for mount in volume_mounts:
                if mount["name"] not in existing_mounts:
                    container["volumeMounts"].append(mount)

with open(filepath, "w") as f:
    yaml.dump_all(docs, f, default_flow_style=False)

print("Updated airflow volumes.")
