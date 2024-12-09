#!/bin/bash

readonly BASE_PATH="/home/dcl/Desktop/CS451-2024-project/template_java/"
readonly RUN_PATH="run.sh"
readonly BUILD_PATH="build.sh"
readonly TC_PATH="../tools/tc.py"
readonly OUTPUT_PATH="../example/output/"
readonly HOSTS_PATH="../example/hosts"
readonly BASE_CONFIG_PATH="../example/configs/"
readonly PREFIX_CONFIG_PATH="lattice-agreement-"
readonly SUFFIX_CONFIG_PATH=".config"
readonly CONFIG_GENERATOR="../example/configs/latice-config-generator.py"
readonly CORRECTNESS_PASS="src/main/java/cs451/tests/verify_correctness.py"
readonly THROUGHPUT_PATH="src/main/java/cs451/tests/compute_throughput.py"

readonly EXEC_TIME=60

# Config gererator parameters
readonly NB_PROCESSES=6 # Nb of processes
readonly P=100 # Nb of rounds
readonly VS=15 # Max nb of values per process
readonly DS=30 # Max nb of distinct values for all processes

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

# Generate the configuration files
echo ""
echo "Generating files..."
python3 $CONFIG_GENERATOR $NB_PROCESSES $P $VS $DS $BASE_PATH$BASE_CONFIG_PATH $BASE_PATH$HOSTS_PATH $BASE_PATH$OUTPUT_PATH
echo "files generated!"
sleep 5



# Start all processes (Correctness/Performance Test)
echo ""
echo "Starting all processes..."
for i in $(seq 1 $NB_PROCESSES);
do
    gnome-terminal -- bash -c "cd $BASE_PATH; ./$RUN_PATH --id $i --hosts $HOSTS_PATH --output $OUTPUT_PATH/$i.output $BASE_CONFIG_PATH$PREFIX_CONFIG_PATH$i$SUFFIX_CONFIG_PATH; exec bash";
done
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
sleep 1  # 10 seconds

# Verify the correctness of the test
echo ""
echo "Verifying the correctness of the test..."
python3 $BASE_PATH$CORRECTNESS_PASS LATICE $BASE_PATH$OUTPUT_PATH $BASE_PATH$BASE_CONFIG_PATH 
echo "Correctness verified!"

# Compute the aggregate throughput by analyzing logs
# echo ""
# echo "Computing throughput from logs..."
# python3 $BASE_PATH$THROUGHPUT_PATH $BASE_PATH$OUTPUT_PATH $EXEC_TIME
# echo "Throughput computed!"

# Kill network setup script
echo ""
echo "Stopping network setup script..."
pkill -f tc.py  # Kills all processes related to `tc.py`
echo "Stopped network setup script!"

echo ""
echo "Test completed!"