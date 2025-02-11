#!/bin/bash

# Navigate to the script directory
cd /host_data/saso/deep_stream_scripts || { echo "Directory not found"; exit 1; }
echo "Navigated to script directory"

# Run the Python script
/usr/bin/python3 saso_exit1_prod.py --lanes 6 || { echo "Failed to run script"; exit 1; }
echo "Script executed successfully"

