#!/bin/bash

pip install -r requirements.txt &> /dev/null

python3.10 sender.py "$@"