import re

path = "/home/zodiac/stadygit/MLOps/infra/k8s/01-databases.yaml"
with open(path, "r") as f:
    data = f.read()

# Fix strict type matching in k8s deployment spec (values inside Quotes not allowed)
data = re.sub(r"initialDelaySeconds:\s*['\"]10['\"]", "initialDelaySeconds: 10", data)
data = re.sub(r"periodSeconds:\s*['\"]10['\"]", "periodSeconds: 10", data)

with open(path, "w") as f:
    f.write(data)

