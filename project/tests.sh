
#!/bin/bash
# Define the test script name
TEST_SCRIPT="project/test.py"

# Define the data directory
DATA_DIR="data"


if [ ! -f "$TEST_SCRIPT" ]; then
    echo "Error: Test script $TEST_SCRIPT not found!"
    exit 1
fi

echo "Starting the test suite for the data pipeline..."

# Run the test script using Python
python3 "$TEST_SCRIPT"
TEST_RESULT=$?

# Check if the tests passed or failed
if [ $TEST_RESULT -eq 0 ]; then
    echo "All tests passed successfully!"
else
    echo "Some tests failed. Please check the output for details."
    exit 1
fi


echo "Automated tests completed."
