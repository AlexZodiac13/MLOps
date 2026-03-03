import yaml
import os

def fix_file(filepath):
    with open(filepath, "r") as f:
        content = f.read()
    
    # 04-nginx: metadata.type -> spec.type
    content = content.replace("  type: LoadBalancer", "")
    content = content.replace("spec:\n  ports:", "spec:\n  type: LoadBalancer\n  ports:")
    
    # 03-airflow: Trailing newline in secretRef
    content = content.replace(r"name: infra-secrets\n", "name: infra-secrets")
    
    with open(filepath, "w") as f:
        f.write(content)

fix_file("/home/zodiac/stadygit/MLOps/infra/k8s/04-nginx.yaml")

import io

def fix_yaml_load(filepath):
    try:
        with open(filepath, "r") as f:
            docs = list(yaml.safe_load_all(f))
            
        for doc in docs:
            if not doc: continue
            
            # 01-databases: Fix livenessProbe types (from string to int) for redis
            if doc.get("kind") == "Deployment" and doc.get("metadata", {}).get("name") == "redis":
                probe = doc["spec"]["template"]["spec"]["containers"][0].get("livenessProbe")
                if probe:
                    probe["initialDelaySeconds"] = int(probe.get("initialDelaySeconds", 10))
                    probe["periodSeconds"] = int(probe.get("periodSeconds", 10))

            # Fix 03-airflow worker trailing \n that got written into yaml
            if doc.get("kind") == "Deployment" and doc.get("metadata", {}).get("name") == "airflow-worker":
                envFrom = doc["spec"]["template"]["spec"]["containers"][0].get("envFrom", [])
                for env in envFrom:
                    if "secretRef" in env:
                        if env["secretRef"]["name"].endswith("\\n"):
                            env["secretRef"]["name"] = env["secretRef"]["name"][:-2]
                        if env["secretRef"]["name"].endswith("\n"):
                            env["secretRef"]["name"] = env["secretRef"]["name"].strip()


        with open(filepath, "w") as f:
            yaml.dump_all(docs, f, default_flow_style=False)
            
    except Exception as e:
        print(f"Failed to fix yaml {filepath}: {e}")

fix_yaml_load("/home/zodiac/stadygit/MLOps/infra/k8s/01-databases.yaml")
fix_yaml_load("/home/zodiac/stadygit/MLOps/infra/k8s/03-airflow.yaml")

