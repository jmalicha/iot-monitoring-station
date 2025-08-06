#!/bin/bash

# Install the required Python modules from the "requirements.txt" file.
# Any error messages are redirected to /dev/null to keep the output clean.
pip install -r requirements.txt &> /dev/null

# Run the "reader.py" script with any arguments passed to this script.
# The "$@" ensures that all command-line arguments provided to "run_reader.sh" are forwarded to "reader.py".
# This allows the user to override the default CLI argument values in "reader.py".
python3 reader.py "$@"