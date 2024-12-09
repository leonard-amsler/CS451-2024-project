import os
import random
import sys

def generate_configs(N, p, vs, ds, PATH):
    """
    Generate N configuration files in PATH.
    Each config file:
      - First line: "p vs ds"
      - Next p lines: each line is a randomly generated proposal (a set of up to vs distinct integers from [1, ds])
    """
    # Ensure the output directory exists
    os.makedirs(PATH, exist_ok=True)

    for i in range(1, N+1):
        filename = os.path.join(PATH, f"lattice-agreement-{i}.config")
        with open(filename, 'w') as f:
            # Write the first line (same for all files)
            f.write(f"{p} {vs} {ds}\n")

            # Generate p proposal lines
            for _ in range(p):
                # Random number of elements in this proposal (from 1 to vs)
                length = random.randint(1, vs)
                # Choose 'length' distinct elements from [1, ds]
                proposal = random.sample(range(1, ds+1), length)
                # Write them as space-separated integers
                f.write(' '.join(map(str, proposal)) + "\n")

    print(f"Generated {N} configuration files in {PATH}")

# Example usage:
if __name__ == "__main__":
    # Parameters
    if(len(sys.argv) != 8):
        print("Usage: python3 latice-config-generator.py <N> <p> <vs> <ds> <output_dir> <hosts file> <output_dir>")
        sys.exit(1)
    N = int(sys.argv[1])
    p = int(sys.argv[2])
    vs = int(sys.argv[3])
    ds = int(sys.argv[4])
    PATH = sys.argv[5]
    hosts = sys.argv[6]
    output_dir = sys.argv[7]

    generate_configs(N, p, vs, ds, PATH)

    # Generate hosts file
    with open(hosts, 'w') as f:
        for i in range(1, N+1):
            if i < 10:
                f.write(f"{i} localhost 1100{i}\n")
            elif i < 100:
                f.write(f"{i} localhost 110{i}\n")
            elif i < 1000:
                f.write(f"{i} localhost 11{i}\n")
            else:
                f.write(f"{i} localhost 1{i}\n")
    print(f"Generated {N} hosts in {hosts}")

    # Remove all files in output_dir
    os.system(f"rm -rf {output_dir}")
    # Create output_dir
    os.makedirs(output_dir, exist_ok=True)
    print(f"Reset output directory {output_dir}")



            
