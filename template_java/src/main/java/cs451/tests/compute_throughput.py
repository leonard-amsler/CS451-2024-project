import os
import sys

def compute_throughput(log_dir, time):
    total_messages = 0

    # Iterate through all log files in the log directory
    for filename in os.listdir(log_dir):
        if filename.endswith(".output"):
            with open(os.path.join(log_dir, filename), 'r') as f:
                lines = f.readlines()

                # Count the number of delivered messages (assuming each line starting with 'd' represents a delivery)
                delivered_messages = [line for line in lines if line.startswith('d')]
                total_messages += len(delivered_messages)

    return total_messages

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 compute_throughput.py <log_directory> <time>")
        sys.exit(1)

    log_directory = sys.argv[1]
    time = int(sys.argv[2])
    throughput = compute_throughput(log_directory, time)

    print(f"Total messages delivered across all processes: {throughput} in {time} seconds")
    print(f"Throughput: {throughput / time} messages/second")
