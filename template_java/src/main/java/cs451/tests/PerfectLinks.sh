#!/bin/bash

readonly BASE_PATH="/home/dcl/Desktop/CS451-2024-project/template_java/"
readonly RUN_PATH="run.sh"
readonly BUILD_PATH="build.sh"
readonly TC_PATH="../tools/tc.py"
readonly OUTPUT_PATH="../example/output/"
readonly HOSTS_PATH="../example/hosts"
readonly PERFECT_LINKS_PATH="../example/configs/perfect-links.config"
readonly THROUGHPUT_PATH="src/main/java/cs451/tests/compute_throughput.py"
readonly CORRECTNESS_PASS="src/main/java/cs451/tests/verify_correctness.py"

readonly EXEC_TIME=60

# Build the application
echo ""
echo "Building the application..."
bash $BASE_PATH$BUILD_PATH
echo "Build finished!"
sleep 5

# Run the network setup script
echo ""
echo "Running network setup script..."
gnome-terminal -- bash -c "cd $BASE_PATH; python $TC_PATH; exec bash"
echo "Network setup script finished!"
sleep 5

# Start all processes (Correctness/Performance Test)
echo ""
echo "Starting all processes..."
gnome-terminal -- bash -c "cd $BASE_PATH; ./$RUN_PATH --id 1 --hosts $HOSTS_PATH --output $OUTPUT_PATH/1.output $PERFECT_LINKS_PATH; exec bash"
gnome-terminal -- bash -c "cd $BASE_PATH; ./$RUN_PATH --id 2 --hosts $HOSTS_PATH --output $OUTPUT_PATH/2.output $PERFECT_LINKS_PATH; exec bash"
gnome-terminal -- bash -c "cd $BASE_PATH; ./$RUN_PATH --id 3 --hosts $HOSTS_PATH --output $OUTPUT_PATH/3.output $PERFECT_LINKS_PATH; exec bash"
gnome-terminal -- bash -c "cd $BASE_PATH; ./$RUN_PATH --id 4 --hosts $HOSTS_PATH --output $OUTPUT_PATH/4.output $PERFECT_LINKS_PATH; exec bash"
gnome-terminal -- bash -c "cd $BASE_PATH; ./$RUN_PATH --id 5 --hosts $HOSTS_PATH --output $OUTPUT_PATH/5.output $PERFECT_LINKS_PATH; exec bash"
echo "Started all processes!"

echo ""
echo "Waiting for $EXEC_TIME seconds..."
sleep $EXEC_TIME  # 1 minutes

# Stop all processes
echo ""
echo "Stopping all processes..."
pkill -f run.sh  # Kills all processes related to `run.sh`
echo "Stopped all processes!"

# Wait y minutes (e.g., 1 minute) for logs to be written
echo ""
echo "Waiting for logs to be written..."
sleep 10  # 10 seconds

# Kill network setup script
echo ""
echo "Stopping network setup script..."
pkill -f tc.py  # Kills all processes related to `tc.py`
echo "Stopped network setup script!"

# Verify the correctness of the test
echo ""
echo "Verifying the correctness of the test..."
python3 $BASE_PATH$CORRECTNESS_PASS $OUTPUT_PATH
echo "Correctness verified!"

# Compute the aggregate throughput by analyzing logs
echo ""
echo "Computing throughput from logs..."
python3 $BASE_PATH$THROUGHPUT_PATH $OUTPUT_PATH $EXEC_TIME
echo "Throughput computed!"

echo ""
echo "Test completed!"