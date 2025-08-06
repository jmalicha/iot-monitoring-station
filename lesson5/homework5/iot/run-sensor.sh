#!/bin/bash

pip install -r requirements.txt &> /dev/null

"$(which python3)" sensor.py "$@"


