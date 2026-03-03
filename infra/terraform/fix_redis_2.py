with open("/home/zodiac/stadygit/MLOps/infra/k8s/01-databases.yaml", "r") as f:
    text = f.read()

text = text.replace("periodSeconds: 10\\n", "periodSeconds: 10")
text = text.replace("initialDelaySeconds: 10\\n", "initialDelaySeconds: 10")
text = text.replace("'10'", "10")

with open("/home/zodiac/stadygit/MLOps/infra/k8s/01-databases.yaml", "w") as f:
    f.write(text)
