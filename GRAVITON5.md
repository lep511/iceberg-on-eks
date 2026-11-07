# AWS Graviton5 Benchmark Suite

A Rust benchmark suite optimized for AWS Graviton5 processors, designed to measure and validate the performance characteristics of the new Neoverse V3 architecture.

## About AWS Graviton5

AWS Graviton5 is Amazon's fifth-generation custom ARM-based processor, launched in June 2026 with the M9g and M9gd EC2 instance families. It represents a major generational leap from Graviton4, doubling the core count and significantly improving the memory subsystem.

### Architecture Overview

Graviton5 is built on Arm's **Neoverse V3** platform with the **Armv9.2-a** instruction set architecture. Each chip integrates **192 physical cores** running at **3.3 GHz** (no SMT — 1 vCPU = 1 physical core). The cores are connected via Arm's **CMN-S3** (Coherent Mesh Network S3) interconnect, replacing the CMN-700 used in Graviton4.

### Graviton4 vs Graviton5 Comparison

| Feature | Graviton4 | Graviton5 | Improvement |
|---------|-----------|-----------|-------------|
| Core | Neoverse V2 | Neoverse V3 | New generation |
| ISA | Armv9.0-a | Armv9.2-a | +2 minor revisions |
| Clock Speed | 2.8 GHz | 3.3 GHz | +18% |
| Core Count | 96 | 192 | 2x |
| L1 Cache | 64KB i + 64KB d | 64KB i + 64KB d | Same |
| L2 Cache | 1 MB/core | 2 MB/core | 2x |
| Last-Level Cache | 36 MB total | 48-96 MB/NUMA | Up to 5x total |
| Memory Channels | 8x DDR5 | 12x DDR5 | +50% |
| Interconnect | CMN-700 | CMN-S3 | New generation |
| SIMD | SVE2 (128-bit x4) | SVE2 (128-bit x4) | Same width |

### ISA Features Supported

- **SVE2** (Scalable Vector Extension 2) with 4x128-bit vector pipelines
- **LSE** (Large System Extensions) atomics for efficient lock-free programming
- **Pointer Authentication** (PAuth/FPAC) for control-flow integrity
- **Branch Target Identification** (BTI) for forward-edge CFI
- **BF16** (BFloat16) for ML inference workloads
- **AFP** (Alternative Floating-Point) behavior
- **WFXT** (Wait For eXtended Timeout) for power-efficient polling

### Features NOT Supported

- **SME/SME2** (Scalable Matrix Extension) — optional in Armv9.2-a but not implemented
- **MOPS** (Memory Operations) — not present in Neoverse V3
- **32-bit ARM** backward compatibility — AArch64 only

### Cache Hierarchy Details

The memory hierarchy on Graviton5 is designed for large-scale parallel workloads:

- **L1**: 64KB instruction + 64KB data per core (same as Graviton4)
- **L2**: 2MB private per core (doubled from Graviton4's 1MB)
- **LLC (Last-Level Cache)**: 48MB per NUMA node for instances up to 16xlarge, 96MB per NUMA node for 24xlarge and larger. Total LLC can reach 192MB on metal instances.
- **Interconnect**: CMN-S3 provides coherent access across NUMA nodes with improved latency characteristics

### NUMA Topology

On larger instance sizes (especially metal-48xl with 192 vCPUs), the cores are distributed across multiple NUMA nodes. For latency-sensitive workloads, pin threads to the same NUMA node. The CMN-S3 interconnect handles cross-node coherency efficiently, but local access is always faster.

## EC2 Instance Types

Graviton5 currently powers the following instance families:

| Instance | Type | Storage | Sizes |
|----------|------|---------|-------|
| **M9g** | General Purpose | EBS-only | medium to metal-48xl |
| **M9gd** | General Purpose | Local NVMe SSD | medium to metal-48xl |

Size range: from `medium` (1 vCPU, 4 GiB RAM) up to `metal-48xl` (192 vCPUs, 768 GiB RAM). Network bandwidth reaches 100 Gbps with 72 Gbps EBS bandwidth at the largest size.

## How to Build and Run

### Prerequisites

- **Rust 1.82+** (ships with LLVM 19, which includes `neoverse-v3` target support)
- An ARM-based system for native compilation, or cross-compilation setup

### Building for Graviton5

```bash
# Maximum performance: target Graviton5 specifically
export RUSTFLAGS="-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse"
cargo build --release
cargo run --release
```

### Cross-Generation Portable Build

```bash
# Balanced flag: produces binaries that run well on Graviton3, 4, and 5
export RUSTFLAGS="-Ctarget-cpu=neoverse-512tvb -Ctarget-feature=+lse"
cargo build --release
```

### Building for Older Graviton Generations

```bash
# For Graviton4 (Neoverse-V2, Armv9.0-a)
export RUSTFLAGS="-Ctarget-cpu=neoverse-v2 -Ctarget-feature=+lse"

# For Graviton3 (Neoverse-V1, Armv8.4-a)
export RUSTFLAGS="-Ctarget-cpu=neoverse-v1 -Ctarget-feature=+lse"

# For Graviton2 (Neoverse-N1, Armv8.2-a)
export RUSTFLAGS="-Ctarget-cpu=neoverse-n1 -Ctarget-feature=+lse"
```

### Compiler Flag Notes

- On ARM64, `-Ctarget-cpu` sets both architecture features and tuning simultaneously (preferred over `-Ctarget-feature` alone)
- The `+lse` feature enables Large System Extensions for atomic operations — critical for multi-threaded performance on 192 cores
- No `graviton5` target exists in LLVM; use `neoverse-v3`
- The `neoverse-v3` target has been backported to GCC-11 on Amazon Linux 2023

## Benchmarks Included

| # | Benchmark | What It Tests |
|---|-----------|---------------|
| 1 | CPU-Bound Computation | FP64 sqrt/sin throughput, single-core IPC |
| 2 | Memory Bandwidth | Sequential read/write with 12-channel DDR5 |
| 3 | Atomic Operations | LSE effectiveness under contention |
| 4 | Multi-threaded Scalability | Thread scaling from 1 to 192 cores |
| 5 | SIMD Operations | Vector add, matrix multiply, FMA throughput |
| 6 | Cache Hierarchy | Pointer-chasing latency at L1/L2/LLC/DRAM levels |
| 7 | Branch Prediction | Predictable vs random branch penalty |
| 8 | JSON Parsing | Character-level parsing throughput |
| 9 | String Operations | Allocation, concatenation, pattern matching |
| 10 | NUMA Awareness | Cross-node memory bandwidth via CMN-S3 |

## Test Results

Tested on **June 25, 2026** on an **m9g.2xlarge** instance (8 vCPUs, 32 GiB RAM, 1 NUMA node).

### System Verification

```
$ lscpu
Architecture:       aarch64
CPU(s):             8
Thread(s) per core: 1
Core(s) per socket: 8
L1d cache:          512 KiB (8 instances = 64 KB/core)
L1i cache:          512 KiB (8 instances = 64 KB/core)
L2 cache:           16 MiB (8 instances = 2 MB/core)
L3 cache:           48 MiB (1 instance)
NUMA node(s):       1
CPU part:           0xd84 (Neoverse V3)
Flags:              ... atomics sve sve2 paca pacg bti afp wfxt bf16 ...
```

### How It Was Executed

```bash
export RUSTFLAGS="-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse"
cargo build --release
cargo run --release
```

### Key Results (m9g.2xlarge, 8 vCPUs)

| Benchmark | Result |
|-----------|--------|
| FP64 Computation | 1.58e8 ops/sec |
| Memory Read BW | 35.17 GB/s |
| Memory Write BW | 5.16 GB/s |
| Atomic ops/sec (LSE) | 1.63e8 |
| SIMD Throughput | 14.05 GB/s |
| Matrix Multiply (512x512) | 2.27 GFLOPS |
| FMA Throughput | 2.47 GFLOPS |
| Cache Latency (L1, 16-64KB) | ~1.21 ns |
| Cache Latency (L2, 256KB-2MB) | 2.79 - 8.79 ns |
| Cache Latency (LLC, 4-48MB) | 16 - 91 ns |
| Cache Latency (DRAM, 64-128MB) | ~92 - 95 ns |
| Branch Misprediction Penalty | <1% |
| JSON Parsing | 784 MB/s |
| String Allocation | 25.29 ns/op |
| Aggregate NUMA BW (8 threads) | 113.31 GB/s |

### Configuration Comparison

Six different compiler configurations were tested to find the optimal setup. Full details in [CONFIG.md](CONFIG.md).

#### Configurations Tested

| # | RUSTFLAGS | Notes |
|---|---|---|
| 1 | *(none)* | Baseline — no target-cpu |
| 2 | `-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse` | Recommended for Graviton5 |
| 3 | `-Ctarget-cpu=native` | Auto-detect CPU |
| 4 | `-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse,+sve2,+i8mm,+bf16,+crc` | Aggressive all-features |
| 5 | `-Ctarget-cpu=neoverse-512tvb -Ctarget-feature=+lse` | Cross-generation (G3/G4/G5) |
| 6 | `-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse,+sve2` + `panic="abort"` | Abort on panic |

#### Performance Improvement: neoverse-v3+LSE vs Baseline

| Benchmark | Baseline | neoverse-v3+LSE | Improvement |
|-----------|----------|-----------------|-------------|
| JSON Parsing | 628 MB/s | 784 MB/s | **+24.8%** |
| Pattern Matching | 3.93 s | 3.14 s | **+20.1%** |
| Atomic Ops/sec | 1.37e8 | 1.63e8 | **+19.0%** |
| Memory Write BW | 4.67 GB/s | 5.16 GB/s | **+10.5%** |

#### Best Configuration Per Workload

| Workload | Best Config | RUSTFLAGS |
|----------|-------------|-----------|
| Server/Web (strings, JSON) | neoverse-v3+LSE | `-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse` |
| Concurrent/Lock-free | 512tvb+LSE | `-Ctarget-cpu=neoverse-512tvb -Ctarget-feature=+lse` |
| Numeric/SIMD (vector ops) | native | `-Ctarget-cpu=native` |
| Cross-generation portable | 512tvb+LSE | `-Ctarget-cpu=neoverse-512tvb -Ctarget-feature=+lse` |
| **General best** | **neoverse-v3+LSE** | **`-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse`** |

### Observations

- LSE and SVE2 correctly detected at compile time with `-Ctarget-cpu=neoverse-v3`
- Cache hierarchy clearly visible with Fisher-Yates shuffled pointer-chasing:
  - L1 hit: ~1.21 ns (4 cycles @ 3.3 GHz)
  - L2 hit: 2.8-8.8 ns (9-29 cycles), transition at 64KB boundary
  - LLC hit: 16-91 ns (grows with distance in CMN-S3 mesh)
  - DRAM: stabilizes at ~95 ns beyond 48MB LLC capacity
- Branch prediction is excellent — <1% penalty on pseudo-random patterns (likely uses branchless CSEL)
- SVE2 is **not always faster** than NEON for simple vector operations (add/multiply). NEON outperforms SVE2 by ~18% on short vectors due to lower instruction overhead
- LSE provides 19-20% improvement for contested atomic operations
- The biggest gains (+25%) come from instruction scheduling optimizations when the compiler knows the exact microarchitecture
- FMA/FP64 transcendental performance is hardware-bound at 2.47 GFLOPS regardless of flags
- Single-thread read bandwidth already saturates at ~35 GB/s with 8 cores
- NUMA bandwidth test shows 113 GB/s aggregate across 8 threads (single NUMA node)

## Intensive Benchmarks (divan + parking_lot)

In addition to the main benchmark binary, the `benches/` directory contains intensive concurrency benchmarks using the [divan](https://crates.io/crates/divan) framework. These compare `std::sync::RwLock` vs `parking_lot::RwLock` under various contention patterns, scaled for Graviton5's 192-core topology.

### Benchmarks in `benches/graviton5_intensive_benchmarks.rs`

| Benchmark | Description |
|-----------|-------------|
| `massive_concurrent_reads` | Pure read contention with heavy computation under lock |
| `reader_heavy` | 95% reads / 5% writes pattern |
| `writer_heavy` | 80% reads / 20% writes pattern |
| `extreme_write_contention` | 100% writes with work under lock |
| `multiple_locks` | Read-all + write-one across 192 independent locks |
| `cache_friendly` | False-sharing avoidance with 128-byte aligned padding |
| `upgradable_read_realistic` | parking_lot exclusive: read-then-upgrade pattern |
| `sustained_throughput` | Heavy read + occasional write stress test |
| `numa_cross_node_contention` | Read from one lock, write to another (CMN-S3 cross-node) |

Each benchmark runs at thread counts: `[1, 2, 4, 8, 16, 32, 64, 96, 128, 192]`

### Running the Intensive Benchmarks

```bash
# Run all concurrency benchmarks
export RUSTFLAGS="-Ctarget-cpu=neoverse-v3 -Ctarget-feature=+lse"
cargo bench

# Run a specific benchmark by name filter
cargo bench -- massive_concurrent_reads

# Run only parking_lot benchmarks
cargo bench -- parking_lot
```

### Dependencies

- **parking_lot 0.12**: Faster RwLock implementation, especially under LSE atomics
- **divan 0.1**: Benchmarking framework with built-in multi-threaded support

## Performance Tips for Graviton5

1. **Always compile with `-Ctarget-cpu=neoverse-v3`** to enable all micro-architectural optimizations and instruction scheduling tuned for Neoverse V3.

2. **Enable LSE** (`-Ctarget-feature=+lse`) for any concurrent code. Without LSE, atomic operations fall back to LL/SC loops that perform significantly worse under contention on 192 cores.

3. **Be NUMA-aware** on large instances. Use `libnuma` or manual thread-pinning to keep threads and their data on the same NUMA node for latency-sensitive paths.

4. **Leverage the 2MB L2 cache**. Algorithms with working sets between 1-2MB per thread will see significant improvement over Graviton4 (which had 1MB L2).

5. **Memory bandwidth scales with threads**. The 12-channel DDR5 subsystem provides ~50% more bandwidth than Graviton4. Use multiple threads reading from separate regions to saturate bandwidth.

6. **Use `neoverse-512tvb`** for binaries that must run on Graviton3, 4, and 5. This is the recommended balanced target for fleet-wide deployment.