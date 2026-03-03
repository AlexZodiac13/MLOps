import os
import glob
import yaml

k8s_dir = "/home/zodiac/stadygit/MLOps/infra/k8s"

for file in glob.glob(os.path.join(k8s_dir, "*.yaml")):
    with open(file, "r") as f:
        data = f.read()
    
    # Fix the weird \n that got injected into names
    data = data.replace('nginx-conf\\n', 'nginx-conf')
    data = data.replace('postgres-init-script\\n', 'postgres-init-script')
    
    docs = list(yaml.safe_load_all(data))
    for doc in docs:
        if not doc: continue
        if doc.get("kind") in ["Deployment", "StatefulSet", "Job", "CronJob"]:
            spec = doc["spec"]["template"]["spec"] if doc.get("kind") != "CronJob" else doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            for vol in spec.get("volumes", []):
                if "configMap" in vol and vol["configMap"].get("name"):
                    if vol["configMap"]["name"].endswith("\\n"):
                        vol["configMap"]["name"] = vol["configMap"]["name"][:-2]
                if "configMap" in vol and vol["configMap"].get("name"):
                    if vol["configMap"]["name"].endswith("\n"):
                        vol["configMap"]["name"] = vol["configMap"]["name"].strip()
            
    with open(file, "w") as f:
        yaml.dump_all(docs, f, default_flow_style=False)

print("Fixed volumes")
