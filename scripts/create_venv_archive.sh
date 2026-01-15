#!/bin/bash

python3.8 -m venv .temp_venv
source .temp_venv/bin/activate
.temp_venv/bin/pip install --upgrade pip
.temp_venv/bin/pip install wheel
.temp_venv/bin/pip install -r requirements-prod.txt
.temp_venv/bin/pip install venv-pack
.temp_venv/bin/python3 -m venv_pack -o venvs/venv.tar.gz --force
rm -rf .temp_venv
