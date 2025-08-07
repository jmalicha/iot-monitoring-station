#!/bin/bash

# Install the required Python modules from the "requirements.txt" file.
# Any error messages are redirected to /dev/null to suppress unwanted output in the terminal.
pip install -r requirements.txt &> /dev/null

# Execute the "writer.py" script, passing any command-line arguments received by this script.
# The "$@" forwards all the arguments provided to "run_writer.sh" to "writer.py", 
# allowing users to override default values in the "writer.py" script.
python3 writer.py "$@"