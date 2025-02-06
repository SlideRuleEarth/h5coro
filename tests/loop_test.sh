#!/bin/bash

# Initialize counters
run_count=0

# Loop until pytest fails
while true; do
    run_count=$((run_count + 1))
    echo "==========================="
    echo "Test Run #$run_count"
    echo "==========================="

    # Run the pytest command
    pytest -s test_multiprocess.py
    exit_code=$?

    # Check the exit code
    if [ $exit_code -ne 0 ]; then
        echo "Test failed on run #$run_count"
        break
    fi
done
