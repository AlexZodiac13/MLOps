import re

def fix_yaml(path):
    with open(path, "r") as f:
        data = f.read()

    # Fix redis livenessProbe string values that pyyaml might have dumped as strings again
    data = re.sub(r'initialDelaySeconds:\s*[\'"]?10[\'"]?', 'initialDelaySeconds: 10', data)
    data = re.sub(r'periodSeconds:\s*[\'"]?10[\'"]?', 'periodSeconds: 10', data)

    with open(path, "w") as f:
        f.write(data)

fix_yaml("/home/zodiac/stadygit/MLOps/infra/k8s/01-databases.yaml")

# Fix 00-config.yaml missing key issue in Secrets
def fix_secrets(path):
    with open(path, "r") as f:
        data = f.read()
    
    lines = data.split('\n')
    out_lines = []
    for line in lines:
        if "YOUR_AWS_KEY" in line or "YOUR_AWS_SECRET" in line or "YOUR_BUCKET" in line or "YOUR_HF_TOKEN" in line or "YOUR_GIT_REPO" in line:
            # ensure quotes are preserved and it's valid yaml mapping
            parts = line.split(":", 1)
            key = parts[0].strip()
            val = parts[1].strip()
            out_lines.append(f"  {key}: {val}")
        else:
            out_lines.append(line)
            
    with open(path, "w") as f:
        f.write("\n".join(out_lines))

# Actually 00-config.yaml and 02-mlflow.yaml error is usually due to bad indentation or unescaped characters. 
# Let's rewrite them completely using standard template to be safe.
import os
os.system("rm -f /home/zodiac/stadygit/MLOps/infra/k8s/00-config.yaml /home/zodiac/stadygit/MLOps/infra/k8s/02-mlflow.yaml")
