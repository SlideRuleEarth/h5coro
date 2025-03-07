#!/bin/bash

# Check if the required parameters are provided
if [ $# -lt 2 ]; then
    echo "Usage: $0 <test_file> [loop_count]"
    exit 1
fi

# Assign parameters
test_file=$1          # Name of the test file (e.g., test_multiprocess.py)
loop_count=${2:--1}   # Loop count, default to -1 (run forever) if not provided
run_count=0

# Check if the test file exists
if [ ! -f "$test_file" ]; then
    echo "Error: Test file '$test_file' not found!"
    exit 1
fi

# Function to determine if the file is a pytest file or a standalone Python script
is_pytest_file() {
    # If the file contains pytest-related code but does NOT contain a main entry point, assume pytest
    if grep -q "pytest" "$test_file" && ! grep -q "__main__" "$test_file"; then
        return 0  # True (use pytest)
    else
        return 1  # False (use python)
    fi
}

# Function to handle Ctrl+C (SIGINT)
_cleanup() {
    # Kill all pytest processes
    pkill -9 -f "pytest"

    # Also, make sure all Python test processes are terminated
    pkill -9 -f "python .*${test_file}"

    # Kill all Python processes related to multiprocessing
    pkill -9 -f "python .*multiprocessing"
}

cleanup() {
    echo "Ctrl+C detected! Stopping tests and killing all related processes..."
    _cleanup
    exit 1
}

# Trap SIGINT (Ctrl+C) to call the cleanup function
trap cleanup SIGINT

# Function to wait for test processes to exit
wait_for_test_exit() {
    local wait_time=0

    if is_pytest_file; then
        process_name="pytest .*${test_file}"
    else
        process_name="python .*${test_file}"
    fi

    while pgrep -f "$process_name" > /dev/null; do
        if [ $wait_time -ge 10 ]; then
            echo "Test processes still running after 10 seconds. Killing them..."
            _cleanup  # Use the cleanup function to kill lingering processes
            break
        fi
        echo "Test processes still running... waiting ($wait_time seconds)"
        sleep 1
        wait_time=$((wait_time + 1))
    done
}

# Loop until the specified loop count or until the test fails
while [ $loop_count -eq -1 ] || [ $run_count -lt $loop_count ]; do
    run_count=$((run_count + 1))
    echo "==========================="
    echo "Test Run #$run_count" of "$loop_count"
    echo "==========================="

    # Run the test
    if is_pytest_file; then
        echo "Running test with pytest..."
        pytest -s "$test_file"
    else
        echo "Running test with python..."
        python "$test_file"
    fi
    exit_code=$?

    # Check the exit code
    if [ $exit_code -ne 0 ]; then
        echo "Test failed on run #$run_count"
        cleanup  # Call cleanup on failure to kill any lingering test processes
    fi

    # If loop count is not -1, stop when the limit is reached
    if [ $loop_count -ne -1 ] && [ $run_count -ge $loop_count ]; then
        break
    fi

    # Check and wait for any lingering test processes to exit
    wait_for_test_exit

done

echo "All $run_count test runs completed successfully."
