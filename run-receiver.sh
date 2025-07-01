#!/bin/bash
python3.10 -m pip install -r requirements.txt &> /dev/null
python3.10 receiver.py "$@"

