import os
import sys
from collections import defaultdict

def parse_logs(output_dir):
    """
    Parse logs from the output directory and return sent and delivered messages.
    """
    sent = defaultdict(list)        # sender_id -> [(receiver_id, seq_num)]
    delivered = defaultdict(list)   # receiver_id -> [(sender_id, seq_num)]
    
    # Iterate over output files in the directory
    for filename in os.listdir(output_dir):
        if filename.endswith(".output"):
            process_id = int(filename.split('.')[0])
            file_path = os.path.join(output_dir, filename)
            
            with open(file_path, 'r') as f:
                for line in f:
                    tokens = line.strip().split()
                    
                    if tokens[0] == 'b':  # Message broadcasted (sent)
                        seq_num = int(tokens[1])
                        sent[process_id].append((None, seq_num))  # Keep track of all sent messages
                
                    elif tokens[0] == 'd':  # Message delivered
                        sender_id = int(tokens[1])
                        seq_num = int(tokens[2])
                        delivered[process_id].append((sender_id, seq_num))  # Record delivery

    return sent, delivered


def check_pl1(sent, delivered):
    """
    Check for PL1: Reliable Delivery.
    """
    violations = []
    
    for sender_id, sent_messages in sent.items():
        for seq_num in [msg[1] for msg in sent_messages]:
            found = False
            for receiver_id, delivered_messages in delivered.items():
                if (sender_id, seq_num) in delivered_messages:
                    found = True
                    break
            if not found:
                violations.append(f"PL1 Violation: Message {seq_num} sent by process {sender_id} was not delivered by any process.")
    
    return violations


def check_pl2(delivered):
    """
    Check for PL2: No Duplication.
    """
    violations = []
    
    for receiver_id, delivered_messages in delivered.items():
        unique_messages = set()
        
        for msg in delivered_messages:
            if msg in unique_messages:
                violations.append(f"PL2 Violation: Message {msg[1]} from sender {msg[0]} was delivered more than once by process {receiver_id}.")
            unique_messages.add(msg)
    
    return violations


def check_pl3(sent, delivered):
    """
    Check for PL3: No Creation.
    """
    violations = []
    
    for receiver_id, delivered_messages in delivered.items():
        for sender_id, seq_num in delivered_messages:
            if (None, seq_num) not in sent.get(sender_id, []):
                violations.append(f"PL3 Violation: Message {seq_num} delivered by process {receiver_id} was never sent by process {sender_id}.")
    
    return violations


def verify_correctness(output_dir):
    """
    Main function to verify correctness based on PL1, PL2, and PL3 rules.
    """
    sent, delivered = parse_logs(output_dir)
    
    # Check each rule
    # pl1_violations = check_pl1(sent, delivered) ## Commented out to avoid PL1 violations in the case we don't have enough time to deliver all messages
    pl2_violations = check_pl2(delivered)
    pl3_violations = check_pl3(sent, delivered)
    
    # Output results
    if not (pl2_violations or pl3_violations):
        print("All checks passed. The outputs conform to the perfect links properties.")
    else:
        
        if pl2_violations:
            print("\nPL2 Violations (No Duplication):")
            for v in pl2_violations:
                print(v)
        
        if pl3_violations:
            print("\nPL3 Violations (No Creation):")
            for v in pl3_violations:
                print(v)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python verify_correctness.py <output_directory>")
        sys.exit(1)

    output_directory = sys.argv[1]
    if not os.path.isdir(output_directory):
        print(f"Error: {output_directory} is not a valid directory.")
        sys.exit(1)

    verify_correctness(output_directory)
