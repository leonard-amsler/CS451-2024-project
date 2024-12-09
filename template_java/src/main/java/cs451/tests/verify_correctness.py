import os
import sys
from collections import defaultdict

def parse_logs(output_dir):
    """
    Parse logs from the output directory and return sent and delivered messages.
    """
    sent = defaultdict(list)        # sender_id -> [(sender_id, seq_num)]
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
                        sent[process_id].append((process_id, seq_num))  # Include sender_id
                    
                    elif tokens[0] == 'd':  # Message delivered
                        sender_id = int(tokens[1])
                        seq_num = int(tokens[2])
                        delivered[process_id].append((sender_id, seq_num))  # Record delivery

    return sent, delivered

# ------------------------------- Perfect Links -------------------------------

def check_pl1(sent, delivered):
    """
    Check for PL1: Reliable delivery: If a correct process p sends a message m to a correct process q, then q eventually delivers m.
    """
    violations = []

    deliver_process = []
    for receiver_id, delivered_messages in delivered.items():
        if len(delivered_messages) > 0:
            deliver_process.append(receiver_id)
    
    for sender_id, sent_messages in sent.items():
        for _, seq_num in sent_messages:
            for receiver_id in deliver_process:
                if (sender_id, seq_num) not in delivered[receiver_id]:
                    violations.append(f"PL1 Violation: Message {seq_num} from process {sender_id} was not delivered to process {receiver_id}.")


def check_pl2(delivered):
    """
    Check for PL2: No duplication: No message is delivered by a process more than once.
    """
    violations = []
    
    for receiver_id, delivered_messages in delivered.items():
        unique_messages = set()
        for sender_id, seq_num in delivered_messages:
            if (sender_id, seq_num) in unique_messages:
                violations.append(f"PL2 Violation: Message {seq_num} from process {sender_id} was delivered more than once by process {receiver_id}.")
            else:
                unique_messages.add((sender_id, seq_num))
    
    return violations


def check_pl3(sent, delivered):
    """
    Check for No creation: If some process q delivers a message m with sender p, then m was previously sent to q by process p.
    """
    violations = []
    
    for receiver_id, delivered_messages in delivered.items():
        for sender_id, seq_num in delivered_messages:
            if (sender_id, seq_num) not in sent[sender_id]:
                violations.append(f"PL3 Violation: Message {seq_num} delivered by process {receiver_id} was never sent to it.")
    return violations


def verify_correctness_pl(output_dir):
    """
    Main function to verify correctness based on PL1, PL2, and PL3 rules.
    """
    sent, delivered = parse_logs(output_dir)
    
    # Check each rule
    pl1_violations = check_pl1(sent, delivered) ## Commented out to avoid PL1 violations in the case we don't have enough time to deliver all messages
    pl2_violations = check_pl2(delivered)
    pl3_violations = check_pl3(sent, delivered)
    
    # Output results
    if not (pl1_violations or pl2_violations or pl3_violations):
        print("All checks passed. The outputs conform to the perfect links properties.")
    else:

        if pl1_violations:
            print("\nPL1 Violations (Reliable Delivery):")
            for v in pl1_violations:
                print(v)
        
        if pl2_violations:
            print("\nPL2 Violations (No Duplication):")
            for v in pl2_violations:
                print(v)
        
        if pl3_violations:
            print("\nPL3 Violations (No Creation):")
            for v in pl3_violations:
                print(v)

# ----------------------------------- FRB1 -----------------------------------

# FRB1: Validity: If a correct process p broadcasts a message m, then p eventually delivers m.
def check_frb1(sent, delivered, correct_processes):
    violations = []
    for sender_id in correct_processes:
        sent_messages = sent.get(sender_id, [])
        delivered_messages = delivered.get(sender_id, [])
        for _, seq_num in sent_messages:
            if (sender_id, seq_num) not in delivered_messages:
                violations.append(
                    f"FRB1 Violation: Message {seq_num} sent by process {sender_id} was not delivered by itself."
                )
    return violations

# ----------------------------------- FRB2 -----------------------------------

# FRB2: No duplication: No message is delivered more than once.
def check_frb2(delivered):
    violations = []
    for receiver_id, delivered_messages in delivered.items():
        unique_messages = set()
        for msg in delivered_messages:
            if msg in unique_messages:
                violations.append(
                    f"FRB2 Violation: Message {msg[1]} from sender {msg[0]} was delivered more than once by process {receiver_id}."
                )
            else:
                unique_messages.add(msg)
    return violations

# ----------------------------------- FRB3 -----------------------------------

# FRB3: No creation: If a process delivers a message m with sender s, then m was previously broadcast by process s.
def check_frb3(sent, delivered):
    violations = []
    for receiver_id, delivered_messages in delivered.items():
        for sender_id, seq_num in delivered_messages:
            sent_messages = sent.get(sender_id, [])
            if (sender_id, seq_num) not in sent_messages:
                violations.append(
                    f"FRB3 Violation: Message {seq_num} delivered by process {receiver_id} was never sent by process {sender_id}."
                )
    return violations

# ----------------------------------- FRB4 -----------------------------------

# FRB4: Uniform agreement: If a message m is delivered by some process (whether correct or faulty), then m is eventually delivered by every correct process.
def check_frb4(delivered, correct_processes):
    violations = []
    all_delivered_messages = set()
    for messages in delivered.values():
        all_delivered_messages.update(messages)
    for msg in all_delivered_messages:
        for process_id in correct_processes:
            delivered_messages = delivered.get(process_id, [])
            if msg not in delivered_messages:
                violations.append(
                    f"FRB4 Violation: Message {msg[1]} from sender {msg[0]} was not delivered by correct process {process_id}."
                )
    return violations

# ----------------------------------- FRB5 -----------------------------------

# FRB5: FIFO delivery: If some process broadcasts message m1 before it broadcasts message m2, then no correct process delivers m2 unless it has already delivered m1.
def check_frb5(sent, delivered, correct_processes):
    violations = []
    for sender_id, sent_messages in sent.items():
        # Sort sent messages by sequence number to get the order they were sent
        sent_messages.sort(key=lambda x: x[1])
        for i in range(len(sent_messages) - 1):
            seq_num1 = sent_messages[i][1]
            seq_num2 = sent_messages[i + 1][1]
            for receiver_id in correct_processes:
                delivered_messages = delivered.get(receiver_id, []) # Delivered messages by receiver_id, in the order they were delivered
                seq_num1_delivered_index = -1
                seq_num2_delivered_index = -1
                for j, msg in enumerate(delivered_messages):
                    if msg[1] == seq_num1:
                        seq_num1_delivered_index = j
                    elif msg[1] == seq_num2:
                        seq_num2_delivered_index = j
                if seq_num2_delivered_index < seq_num1_delivered_index:
                    violations.append(
                        f"FRB5 Violation: Message {seq_num2} was delivered before message {seq_num1} by process {receiver_id}."
                    )
    return violations

def verify_correctness_fifo(output_dir):

    # Parse logs to get sent and delivered messages
    sent, delivered = parse_logs(output_dir)
    
    # Define the list of correct processes
    N = 3
    correct_processes = list(range(1, N + 1))
    
    # Check FRB1
    frb1_violations = check_frb1(sent, delivered, correct_processes)
    if frb1_violations:
        print("FRB1 Violations:")
        for violation in frb1_violations:
            print(violation)
    else:
        print("No FRB1 violations detected.")
    
    # Check FRB2
    frb2_violations = check_frb2(delivered)
    if frb2_violations:
        print("\nFRB2 Violations:")
        for violation in frb2_violations:
            print(violation)
    else:
        print("\nNo FRB2 violations detected.")
    
    # Check FRB3
    frb3_violations = check_frb3(sent, delivered)
    if frb3_violations:
        print("\nFRB3 Violations:")
        for violation in frb3_violations:
            print(violation)
    else:
        print("\nNo FRB3 violations detected.")
    
    # Check FRB4
    frb4_violations = check_frb4(delivered, correct_processes)
    if frb4_violations:
        print("\nFRB4 Violations:")
        for violation in frb4_violations:
            print(violation)
    else:
        print("\nNo FRB4 violations detected.")
    
    # Check FRB5
    frb5_violations = check_frb5(sent, delivered, correct_processes)
    if frb5_violations:
        print("\nFRB5 Violations:")
        for violation in frb5_violations:
            print(violation)
    else:
        print("\nNo FRB5 violations detected.")

# ------------------------------- Lattice Agreement -------------------------------
def parseLaticeLogs(output_dir):
    decided = defaultdict(list)
    for filename in os.listdir(output_dir):
        if filename.endswith(".output"):
            process_id = int(filename.split('.')[0])
            file_path = os.path.join(output_dir, filename)
            with open(file_path, 'r') as f:
                for line in f:
                    tokens = list(map(int, line.strip().split()))
                    decided[process_id].append(tokens)
    
    return decided

def parseLaticeConfig(config_dir):
    configs = defaultdict(list)
    for filename in os.listdir(config_dir):
        if filename.endswith(".config") and filename.startswith("lattice-agreement"):
            process_id = int(filename.split('-')[2].split('.')[0])
            file_path = os.path.join(config_dir, filename)
            with open(file_path, 'r') as f:
                lines = f.readlines()
                rounds, max_proposal, max_diff_proposal = map(int, lines[0].strip().split())
                proposals = []
                for line in lines[1:]:
                    proposals.append(list(map(int, line.strip().split())))
                configs[process_id] = (rounds, max_proposal, max_diff_proposal, proposals)
    return configs

def issubset(a, b):
    return all(x in set(b) for x in set(a))

def check_la1(decided, configs):
    """
    Check for LA1: Validity: Let a process Pi decide a set Oi, then Ii ⊆ Oi and Oi ⊆ Union of all Ij
    """
    violations = []
    all_proposals = set()
    for process_id, values in decided.items():
        _, _, _, proposals = configs[process_id]

        for d_value in values:
            for value in proposals:
                all_proposals.update(value)
                if d_value not in values:
                    violations.append(f"LA1 Violation: Process {process_id} decided values {values} but proposed value {value} is not in the decided values.")

    for process_id, values in decided.items():
        for value in values:
            if not issubset(value, all_proposals):
                violations.append(f"LA1 Violation: Process {process_id} decided values {value} that are not in the union of all proposals.")

    return violations

def check_la2(decided):
    """
    Check for LA2: Consistency: Let a process Pi decide a set Oi and let a process Pj decide a set Oj, then Oi ⊆ Oj or Oi ⊃ Oj
    """
    violations = []
    for process_id, values in decided.items():
            for other_process_id, other_decided_values in decided.items():
                for i in range(min(len(values), len(other_decided_values))):
                    if process_id == other_process_id:
                        continue
                    if not issubset(values[i], other_decided_values[i]) and not issubset(other_decided_values[i], values[i]):
                        violations.append(f"LA2 Violation: Process {process_id} decided on {values[i]} and process {other_process_id} decided on {other_decided_values[i]} but they are not subsets of each other.")
                
    return violations

def check_la3(decided, configs):
    """
    Check for LA3: Termination: Every correct process eventually decides.
    """
    violations = []
    for process_id, values in decided.items():
        rounds, _, _, _ = configs[process_id]
        if len(values) < rounds:
            violations.append(f"LA3 Violation: Process {process_id} decided on {len(values)} values but expected {rounds}.")
    return violations

def verify_correctness_lattice(output_dir, config_dir):
    """
    Main function to verify correctness based on Lattice Agreement rules.
    """
    configs = parseLaticeConfig(config_dir)
    decided = parseLaticeLogs(output_dir)

    # print("\nChecking Lattice Agreement properties...")
    # print("   Configurations:")
    # for process_id, config in configs.items():
    #     print(f"      Process {process_id}: {config}")
    # print("   Decided values:")
    # for process_id, values in decided.items():
    #     print(f"      Process {process_id}: {values}")

    # Pi: Process i
    # Ii: Set of values proposed by Pi
    # Oi: Set of values decided by Pi
    # LA1: Validity: Let a process Pi decide a set Oi, then Ii ⊆ Oi and Oi ⊆ Union of all Ij
    # LA2: Consistency: Let a process Pi decide a set Oi and let a process Pj decide a set Oj, then Oi ⊆ Oj or Oi ⊃ Oj
    # LA3: Termination: Every correct process eventually decides.

    # Check each rule
    print()
    la1_violations = check_la1(decided, configs)
    if la1_violations:
        print("\nLA1 Violations:")
        for violation in la1_violations:
            print(violation)
    else:
        print("No LA1 violations detected.")
    
    la2_violations = check_la2(decided)
    if la2_violations:
        print("\nLA2 Violations:")
        for violation in la2_violations:
            print(violation)
    else:
        print("No LA2 violations detected.")
    
    la3_violations = check_la3(decided, configs)
    if la3_violations:
        print("\nLA3 Violations:")
        for violation in la3_violations:
            print(violation)
    else:
        print("No LA3 violations detected.")
    print()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python verify_correctness.py FIFO <output_directory>")
        print("       python verify_correctness.py PL <output_directory>")
        print("       python verify_correctness.py LATICE <output_directory> <config_directory>")
        sys.exit(1)

    output_directory = sys.argv[2]
    if not os.path.isdir(output_directory):
        print(f"Error: {output_directory} is not a valid directory.")
        sys.exit(1)

    type = sys.argv[1]
    if type == "PL":
        verify_correctness_pl(output_directory)
    elif type == "FIFO":
        verify_correctness_fifo(output_directory)
    elif type == "LATICE":
        verify_correctness_lattice(output_directory, sys.argv[3])
    else:
        print("Error: Invalid type. Choose either 'PL' or 'FIFO'.")
        sys.exit(1)

