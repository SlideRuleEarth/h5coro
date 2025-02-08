#!/bin/bash

# Get the loop count from the command-line argument; default to -1 (run forever) if not provided
loop_count=${1:--1}  # Default to -1 if no parameter is passed
run_count=0

# Function to handle Ctrl+C (SIGINT)
cleanup() {
    echo "Ctrl+C detected! Stopping tests and killing all pytest processes..."
    pkill -9 pytest  # Kill all pytest processes
    exit 1
}

# Trap SIGINT (Ctrl+C) to call the cleanup function
trap cleanup SIGINT

# Loop until the specified loop count or until pytest fails
while [ $loop_count -eq -1 ] || [ $run_count -lt $loop_count ]; do
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
        cleanup  # Call cleanup on failure to kill any lingering pytest processes
    fi

    # If loop count is not -1, stop when the limit is reached
    if [ $loop_count -ne -1 ] && [ $run_count -ge $loop_count ]; then
        break
    fi
done

echo "All $run_count test runs completed successfully."
