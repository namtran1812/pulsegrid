# PulseGrid

PulseGrid is a low-latency state publication system for structured real-time data. It explores how close cross-process state publication can get to an ordinary memory write while still providing sequencing, backpressure handling, recovery, and explicit publication identity.

The current implementation focuses on making the local data plane fast and its state semantics precise before adding distributed replication or adaptive encoding.

## Architecture

    Producer
       |
       | Update
       v
    BackpressurePublisher
       |
       | Delta / coalesced transaction
       v
    TransportFrame
       |
       v
    Shared-Memory SPSC Queue
       |
       v
    FrameDecoder
       |
       | epoch + decoded publication
       v
    StateStore

The normal publication path uses a lock-free single-producer/single-consumer shared-memory ring. State and recovery semantics live above the generic transport layer.

## Update Model

PulseGrid's current fixed-width `Update` contains:

    table_id
    row_id
    column_id
    value type
    sequence
    payload

A logical cell is identified by:

    (table_id, row_id, column_id)

`Update` is currently 32 bytes and 32-byte aligned. Sequence zero is reserved by the state protocol.

## Shared-Memory Transport

The local transport is a bounded SPSC ring backed by POSIX shared memory.

The queue uses:

- producer-owned monotonic `head`
- consumer-owned monotonic `tail`
- 64-bit cursors
- power-of-two capacity and masked indexing
- cache-line-separated cursors
- acquire/release publication
- single and batched push/pop operations

A failed push never overwrites unread data.

Batch operations acquire the opposite cursor once and release the owned cursor once for the complete batch.

### Initialization

The queue initialization state is:

    Uninitialized
          |
          v
     Initializing
          |
          v
        Ready

The creator initializes metadata and cursors before publishing `Ready` with release ordering.

An attaching process acquire-loads the initialization state before consuming that metadata.

Attachment validates:

- protocol magic
- protocol version
- queue capacity
- payload size
- payload alignment
- nonzero publication epoch

Payload size and alignment are ABI checks, not complete runtime type identity.

The current shared-memory protocol version is `3`.

## Transport Frames

State-aware publication uses a fixed-size `TransportFrame`.

Frame kinds are:

    Delta
    CoalescedBegin
    CoalescedCell
    CoalescedEnd

`TransportFrame` remains 64 bytes and 32-byte aligned.

Publication epoch is deliberately not repeated in every frame. It is stored once in shared-memory session metadata.

The consumer obtains it through:

    ShmHeader.publication_epoch
                 |
                 v
           Queue::attach
                 |
                 v
     queue.publication_epoch()
                 |
                 v
          FrameDecoder(epoch)
                 |
                 v
       decoded publication
                 |
                 v
             StateStore

This keeps publication identity out of the per-frame fast path.

## Sequenced State

`StateStore` reconstructs the latest value of every logical cell.

Its state is conceptually:

    publication epoch
    global sequence watermark
    latest cell values

Within an epoch, ordinary deltas must be contiguous.

If the current watermark is `N`, the next ordinary delta must be:

    N + 1

A publication at or below the watermark is stale.

A publication above `N + 1` is a sequence gap.

A publication from another epoch is an epoch mismatch.

Rejected publications do not intentionally mutate logical state.

## Snapshots

A snapshot contains:

    epoch
    sequence watermark
    latest state for each retained cell

Restoring a snapshot directly establishes its epoch and watermark because a snapshot represents compact latest state rather than the complete publication history.

Snapshot validation rejects invalid epochs, invalid watermarks, cells beyond the watermark, and duplicate logical cells.

The all-zero empty snapshot represents an unbound empty store.

## Backpressure and Coalescing

The low-level shared-memory queue handles backpressure by returning failure when full.

State-aware recovery from backpressure lives above the queue.

Once `BackpressurePublisher` cannot enqueue an ordinary delta, subsequent updates enter a bounded coalescing buffer.

The buffer retains the newest pending update for each logical cell while preserving the represented global sequence interval.

For example:

    101  A = 10
    102  B = 20
    103  A = 11
    104  A = 12
    105  B = 21

can be represented as:

    range: 101..105

    A = 12 @ 104
    B = 21 @ 105

A consumer at watermark `100` can apply that coalesced transaction and advance to watermark `105`.

An ordinary delta cannot make the same sequence jump.

Once backpressure begins, newer publications cannot leapfrog pending state. Failed flushes preserve the pending state.

A successfully encoded coalesced transaction is inserted into the shared-memory queue as a batch so the queue publishes the transaction with one head update.

## Publication Epochs

Sequence numbers alone cannot distinguish a restarted producer from the previous producer history.

For example:

    epoch A
      seq 1
      seq 2
      ...
      seq 1000

         crash

    epoch B
      seq 1
      seq 2
      ...

Without publication identity, `B:1` would appear stale relative to `A:1000`.

PulseGrid therefore identifies a position in publication history as:

    (epoch, sequence)

Epoch zero is reserved.

A producer history has one stable nonzero publication epoch. A new history after restart uses a different epoch.

Sequence ordering is interpreted within an epoch.

## Crash and Restart Recovery

A producer restart is not treated as an ordinary sequence transition.

The state lifecycle is:

    Normal
    (epoch E, seq N)
           |
           | producer crash
           v
    Recovery Required
           |
           | publication from E'
           X rejected
           |
           | restore Snapshot(E', W)
           v
    Normal
    (epoch E', seq W)
           |
           | E':W+1
           v
    Normal

Observing a new shared-memory publication epoch does not itself authorize mutation of existing state.

If `StateStore` currently represents epoch `E`, a publication from `E'` is rejected.

The history transition occurs only through explicit recovery:

    restore Snapshot(E', W)

After recovery:

- `E':W+1` can continue normally
- delayed publications from epoch `E` are rejected
- the previous producer history cannot mutate recovered state

The crash/restart integration test exercises this using actual processes, POSIX shared memory, `SIGKILL`, a new producer incarnation, sequence reset, explicit snapshot recovery, and rejection of a delayed old-epoch publication.

## Queue Incarnation vs Publication Epoch

A shared-memory queue incarnation and publication epoch are distinct concepts.

A queue incarnation identifies a transport object.

A publication epoch identifies a logical producer history.

Reconnecting to the same live history should not require a new publication epoch merely because a transport connection changes.

Conversely, a restarted publication history must not silently continue the previous epoch merely because a shared-memory name is reused.

## Recovery Boundary

Snapshot recovery is currently modeled as an out-of-band control-plane operation.

Snapshots are not serialized into the normal 64-byte `TransportFrame` path.

This is intentional: recovery is rare relative to ordinary publication, so recovery machinery should not increase the cost of every fast-path update.

Automatic same-name shared-memory reconnection and snapshot transport are not implemented yet.

## Benchmarks

PulseGrid currently benchmarks shared-memory transport against a Unix-domain `SOCK_STREAM` baseline.

The benchmark suite includes:

    shm_throughput
    uds_throughput

    shm_batched_throughput
    uds_batched_throughput

    shm_latency
    uds_latency

For the repeated batched-throughput measurements:

    warmup:    131,072 updates
    measured:  8,388,608 updates

Across 10 trials on Apple Silicon/macOS:

| Batch | SHM median | UDS median | SHM / UDS |
|------:|-----------:|-----------:|----------:|
| 1 | 25.97M updates/s | 2.008M updates/s | 12.93x |
| 16 | 192.25M updates/s | 24.68M updates/s | 7.79x |
| 64 | 212.87M updates/s | 67.83M updates/s | 3.14x |
| 256 | 230.33M updates/s | 48.97M updates/s | 4.70x |

The latency benchmark uses a ping-pong workload with at most one update in flight.

Observed median round-trip latency:

    Shared memory:       250 ns
    Unix-domain socket:  3.625 us

That is approximately `14.5x` lower median round-trip latency for the measured shared-memory path.

These are round-trip measurements and are not divided by two.

The SHM and UDS paths use different acknowledgement mechanisms, so the latency results should be interpreted as system-level comparisons rather than isolated primitive-to-primitive measurements.

## Correctness

The current suite contains 85 tests covering:

- SPSC FIFO behavior
- bounded/full queue behavior
- wraparound
- concurrent producer/consumer operation
- POSIX shared-memory regions
- cross-process transport
- batched transport
- payload ABI validation
- sequence gaps and stale publications
- snapshot restoration
- coalescing
- state-safe backpressure
- framed transport
- publication epoch propagation
- epoch mismatch rejection
- cross-process framed state reconstruction
- crash/restart recovery

The crash/restart path is also stress-tested repeatedly across real process lifecycles.

## Current Scope

PulseGrid currently provides:

    lock-free local SPSC transport
    cross-process POSIX shared memory
    batched publication
    sequenced state reconstruction
    snapshots
    bounded state-aware coalescing
    backpressure recovery
    framed transactions
    publication epochs
    explicit crash/restart recovery

Not yet implemented:

    automatic same-name reconnection
    snapshot/control-plane transport
    adaptive representation
    LZ4/Zstd compression
    variable-width values
    multi-producer publication
    distributed node-to-node replication
    incremental computation
    Python bindings

## Next: Adaptive Representation

The next phase will measure when structured packing is worthwhile before introducing compression.

The initial comparison will be:

    fixed-width raw Update
              vs.
    packed-column representation

The benchmark will measure:

    encoded bytes per update
    encode cost
    decode cost
    throughput
    batch-size break-even
    payload-shape break-even

Compression will be added only after those measurements establish where it is beneficial.

## Build

Requirements:

- C++23 compiler
- CMake
- GoogleTest for tests

Configure and build:

    cmake -S . -B build \
      -G Ninja \
      -DCMAKE_BUILD_TYPE=Release

    cmake --build build -j

Run tests:

    ctest \
      --test-dir build \
      --output-on-failure

