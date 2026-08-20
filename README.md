# Reliable Broadcast and Lattice Agreement

Building blocks for a decentralized system, implemented in Java over UDP for the EPFL
**CS-451 — Distributed Algorithms** course project. Three layers stack on top of each other,
each corresponding to one milestone:

1. **Perfect links** — reliable, exactly-once delivery over an unreliable channel
2. **FIFO uniform reliable broadcast** — agreement on a per-sender ordered stream
3. **Lattice agreement** — repeated agreement on a growing set of values

Processes communicate over UDP only; the network is lossy, reordering, and delaying, and up to
`f = (n - 1) / 2` processes may crash. Correctness must hold regardless.

## Implementation

All code is under [`template_java/src/main/java/cs451/`](template_java/src/main/java/cs451/).

### Perfect links — [`PerfectLinks.java`](template_java/src/main/java/cs451/PerfectLinks.java)

A stubborn-send-plus-deduplicate link over a single `DatagramSocket`.

- **Batching.** Up to 8 messages (`Constants.BATCH_SIZE`) are packed into one UDP packet, so
  acknowledgements are amortized over a batch rather than paid per message.
- **Retransmission.** Unacknowledged packets sit in a map keyed by sequence number with a send
  timestamp; a scheduled executor re-sends anything past the timeout. An entry is dropped only
  once its ACK arrives.
- **Adaptive window.** A background thread samples the ACK-to-timeout ratio once per timeout
  interval and resizes the in-flight window multiplicatively: halve it below a 40% ACK rate,
  double it above 60%, clamped to [2, 131072] and starting at 256. This keeps throughput up on
  a quiet network without collapsing under loss.
- **Exactly-once delivery.** A concurrent set of delivered messages filters duplicates
  introduced by retransmission, so the upper layer sees each message once.

Three threads run concurrently — receive, send-queue drain, and window adjustment — with a
bounded queue providing backpressure to the layer above.

### FIFO uniform reliable broadcast — [`URB.java`](template_java/src/main/java/cs451/URB.java)

Best-effort broadcast is relayed through perfect links, with uniform agreement built on top:

- Each message carries its original sender and a per-sender timestamp.
- On first receipt of a message it was not the origin of, a process re-broadcasts it, so a
  message that reaches any correct process reaches all of them.
- An acknowledgement set per message tracks which processes have relayed it (the originator
  included). A message becomes deliverable once at least `⌈n/2⌉` processes appear in that set —
  the uniformity condition.
- FIFO order is enforced by delivering a sender's message with timestamp `t` only after `t - 1`
  has been delivered. The delivery thread groups pending messages by sender and walks each
  group in timestamp order, stopping at the first gap.

### Lattice agreement — [`Latice.java`](template_java/src/main/java/cs451/Latice.java)

The proposer/acceptor algorithm from the course, where processes agree on a set of integers such
that all decided sets are comparable by inclusion.

- A proposer broadcasts `⟨proposal, proposed_value, proposal_number⟩`.
- An acceptor replies `ack` when the incoming proposal is a superset of its accepted value;
  otherwise it merges the two, replies `nack`, and returns its own accepted set.
- On `nack_count > 0` and `ack_count + nack_count ≥ f + 1`, the proposer bumps its proposal
  number and re-broadcasts the union it has accumulated.
- On `ack_count ≥ f + 1`, it decides.

Two verification threads poll these two conditions across rounds. Rather than run one round at a
time, up to 10 rounds (`MAX_NB_ACTIVE_ROUNDS`) are kept in flight concurrently, each with its own
proposal number, ack/nack counters and value sets in concurrent maps — a substantial throughput
win when the configuration asks for hundreds of rounds. Decisions are still emitted in round
order, since a round only decides when it is the lowest active one.

[`Main.java`](template_java/src/main/java/cs451/Main.java) as checked in runs the lattice
agreement milestone; the perfect-links and broadcast layers remain in place underneath it.

## Build and run

Requires JDK 11+ and Maven. Build the fat jar:

```sh
cd template_java && ./build.sh
```

Run one process:

```sh
./run.sh --id ID --hosts HOSTS --output OUTPUT CONFIG
```

- `HOSTS` lists every process as `id address port`, one per line — see [`example/hosts`](example/hosts).
- `CONFIG` for lattice agreement starts with `p vs ds` (rounds, max values per proposal, max
  distinct values) followed by one proposal per round — see
  [`example/configs/`](example/configs/).
- `OUTPUT` is where the process logs `b`/`d` (broadcast/deliver) events, which is what the
  correctness checkers parse.

Processes must be started separately, one per id; they run until interrupted with `SIGINT`/`SIGTERM`.

## Testing

| Tool | Purpose |
| --- | --- |
| [`tests/PerfectLinks.sh`](template_java/src/main/java/cs451/tests/PerfectLinks.sh), [`FifoBroadcast.sh`](template_java/src/main/java/cs451/tests/FifoBroadcast.sh), [`Latice.sh`](template_java/src/main/java/cs451/tests/Latice.sh) | End-to-end runs per milestone: build, shape the network, generate configs, launch processes, then check output |
| [`tests/verify_correctness.py`](template_java/src/main/java/cs451/tests/verify_correctness.py) | Parses per-process logs and checks the delivery properties hold |
| [`tests/compute_throughput.py`](template_java/src/main/java/cs451/tests/compute_throughput.py) | Measures messages delivered per second |
| [`tools/stress.py`](tools/stress.py) | Course stress tester — crashes and delays processes mid-run |
| [`tools/tc.py`](tools/tc.py) | Applies loss, delay and reordering to the loopback interface |
| [`example/configs/latice-config-generator.py`](example/configs/latice-config-generator.py) | Generates randomized lattice-agreement configs |

The per-milestone shell scripts contain absolute paths from the machine they were written on and
need their `BASE_PATH` adjusted before use.

## Repository layout

| Path | Contents |
| --- | --- |
| [`template_java/src/main/java/cs451/`](template_java/src/main/java/cs451/) | Implementation: link, broadcast and agreement layers, message encoding, config parsers |
| [`template_java/src/main/java/cs451/tests/`](template_java/src/main/java/cs451/tests/) | Test drivers and log checkers |
| [`example/`](example/) | Sample `hosts` file and per-milestone configs |
| [`tools/`](tools/) | Course-provided stress and network-shaping tools |
