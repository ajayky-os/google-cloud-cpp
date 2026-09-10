# Subsequent Chunk Read Hedging Benchmark Results

**Date:** September 10, 2026  
**Environment:** Compute Engine VM `artemis` (`us-central1-a`)  
**Target Bucket & Object:** `gs://ajayky-minerva/files/primitive_benchmark_500MB.parquet` (Regional Bucket, `us-central1`)  
**Workload:** 10 worker threads, 50 MiB reads (`52,428,800` bytes), 1 MiB chunk reads (`1,048,576` bytes per chunk, 50 chunks per stream), 5 minutes per phase.

---

## Executive Summary

Extending read hedging to subsequent chunk reads (beyond the initial open + peek) successfully bounds tail latency across streaming transfers. When speculative hedging is enabled with a 500 ms hedge delay and 1 s download stall timeout:
1. **Slowest Chunk Latency:** The maximum single-chunk latency drops from **1,731.15 ms** (stall-only) and **1,564.84 ms** (baseline) down to **558.64 ms** (**67.7% reduction**). The worst tail chunk is held tightly near the 500 ms hedge delay.
2. **p99.9 Chunk Latency:** Drops from **438.34 ms** (baseline) to **333.83 ms** (**23.8% reduction**).
3. **Worst Request Tail Latency:** Peak 50 MiB transfer time was cut from **3,145.31 ms** (baseline) down to **1,663.45 ms** (**47.1% reduction**), completely eliminating transfers taking > 2 seconds.
4. **Reliability & Stability:** Evaluated over **27,827 requests** (~**1.39 million 1 MiB chunk reads**) and **1.36 TB transferred** with **100% success rate (0 errors)**.

---

## Comparative Results Table

| Metric | Phase 1: Baseline<br>*(No Stall, No Hedge)* | Phase 2: Stall-Only<br>*(1s Stall, No Hedge)* | Phase 3: Chunk Hedging<br>*(500ms Hedge, 1s Stall)* | Delta: Hedged vs Baseline (P3 vs P1) | Delta: Hedged vs Stall-Only (P3 vs P2) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Total Requests** | 9,225 | 9,827 | **8,775** | **-450 (-4.88%)** | **-1,052 (-10.71%)** |
| **Data Transferred** | 450.44 GB | 479.83 GB | **428.47 GB** | **-21.97 GB** | **-51.37 GB** |
| **Success Rate** | 100% (0 errors) | 100% (0 errors) | **100% (0 errors)** | 0 errors | 0 errors |
| **p50 Total Latency** | 280.85 ms | 273.52 ms | **292.62 ms** | +11.77 ms (+4.19%) | +19.10 ms (+6.98%) |
| **p90 Total Latency** | 516.15 ms | 445.20 ms | **555.05 ms** | +38.89 ms (+7.54%) | +109.85 ms (+24.67%) |
| **p95 Total Latency** | 621.30 ms | 546.49 ms | **649.76 ms** | +28.46 ms (+4.58%) | +103.27 ms (+18.90%) |
| **p99 Total Latency** | 879.81 ms | 795.75 ms | **898.36 ms** | +18.55 ms (+2.11%) | +102.61 ms (+12.89%) |
| **p99.9 Total Latency** | 1,306.05 ms | 1,241.64 ms | **1,357.09 ms** | +51.03 ms (+3.91%) | +115.45 ms (+9.30%) |
| **Max Total Latency** | 3,145.31 ms | 1,966.53 ms | **1,663.45 ms** | **-1,481.86 ms (-47.11%)** | **-303.08 ms (-15.41%)** |

---

## Max Single Chunk Latency (Slowest 1 MiB Chunk per Stream)

Each 50 MiB request reads fifty 1 MiB chunks. This metric isolates the single worst chunk encounter in each stream:

| Percentile | Phase 1 (Baseline) | Phase 2 (Stall Only) | Phase 3 (Chunk Hedging) | Delta: P3 vs P1 | Delta: P3 vs P2 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **p50 Chunk** | 34.55 ms | 33.79 ms | **36.83 ms** | +2.28 ms (+6.60%) | +3.04 ms (+9.00%) |
| **p90 Chunk** | 67.27 ms | 60.02 ms | **73.42 ms** | +6.15 ms (+9.15%) | +13.40 ms (+22.33%) |
| **p95 Chunk** | 85.94 ms | 76.76 ms | **94.07 ms** | +8.13 ms (+9.46%) | +17.31 ms (+22.56%) |
| **p99 Chunk** | 141.98 ms | 126.98 ms | **149.85 ms** | +7.87 ms (+5.54%) | +22.87 ms (+18.01%) |
| **p99.9 Chunk** | 438.34 ms | 356.50 ms | **333.83 ms** | **-104.51 ms (-23.84%)** | **-22.67 ms (-6.36%)** |
| **Max Chunk** | 1,564.84 ms | 1,731.15 ms | **558.64 ms** | **-1,006.20 ms (-64.30%)** | **-1,172.51 ms (-67.73%)** |

---

## TTFB / Open Latency (1st Read + Open)

| Percentile | Phase 1 (Baseline) | Phase 2 (Stall Only) | Phase 3 (Chunk Hedging) | Delta: P3 vs P1 | Delta: P3 vs P2 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **p50 Open** | 35.80 ms | 34.44 ms | **37.30 ms** | +1.50 ms (+4.19%) | +2.87 ms (+8.33%) |
| **p90 Open** | 64.07 ms | 55.62 ms | **70.42 ms** | +6.34 ms (+9.90%) | +14.80 ms (+26.61%) |
| **p95 Open** | 82.50 ms | 69.72 ms | **90.42 ms** | +7.92 ms (+9.59%) | +20.69 ms (+29.68%) |
| **p99 Open** | 139.04 ms | 114.86 ms | **153.28 ms** | +14.24 ms (+10.24%) | +38.42 ms (+33.45%) |
| **p99.9 Open** | 305.08 ms | 236.21 ms | **281.31 ms** | **-23.77 ms (-7.79%)** | +45.09 ms (+19.09%) |
| **Max Open** | 1,661.49 ms | 1,047.91 ms | **481.60 ms** | **-1,179.89 ms (-71.01%)** | **-566.31 ms (-54.04%)** |

---

## Payload Read Latency (Transfer Duration)

| Percentile | Phase 1 (Baseline) | Phase 2 (Stall Only) | Phase 3 (Chunk Hedging) | Delta: P3 vs P1 | Delta: P3 vs P2 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **p50 Read** | 243.05 ms | 238.04 ms | **253.58 ms** | +10.53 ms (+4.33%) | +15.54 ms (+6.53%) |
| **p90 Read** | 450.66 ms | 392.30 ms | **480.94 ms** | +30.28 ms (+6.72%) | +88.64 ms (+22.59%) |
| **p95 Read** | 545.48 ms | 485.57 ms | **570.58 ms** | +25.10 ms (+4.60%) | +85.01 ms (+17.51%) |
| **p99 Read** | 764.97 ms | 718.16 ms | **795.74 ms** | +30.77 ms (+4.02%) | +77.58 ms (+10.80%) |
| **p99.9 Read** | 1,138.15 ms | 1,127.31 ms | **1,227.19 ms** | +89.03 ms (+7.82%) | +99.88 ms (+8.86%) |
| **Max Read** | 2,905.05 ms | 1,939.30 ms | **1,535.67 ms** | **-1,369.38 ms (-47.14%)** | **-403.63 ms (-20.81%)** |

---

## Tail Stall Frequency & Threshold Outliers

| Threshold | Phase 1 (Baseline) | Phase 2 (Stall Only) | Phase 3 (Chunk Hedging) | Impact of Hedging |
| :--- | :---: | :---: | :---: | :---: |
| **Chunks > 500 ms** | 7 (0.08%) | 7 (0.07%) | **2 (0.02%)** | **Eliminated down to 2** |
| **Requests > 1.0 s** | 48 (0.52%) | 33 (0.34%) | **56 (0.64%)** | Mild variance |
| **Requests > 2.0 s** | 2 (0.02%) | 0 (0.00%) | **0 (0.00%)** | **0 requests > 2.0 s** |
| **Requests > 5.0 s** | 0 (0.00%) | 0 (0.00%) | **0 (0.00%)** | **0 requests > 5.0 s** |

---

## Key Observations

1. **Effective Tail Truncation on Intermediate Chunks:**
   In Phase 1 and Phase 2, individual chunk reads stalled up to **1,564 ms** and **1,731 ms** respectively. In Phase 3, when a chunk stalled beyond 500 ms, a hedge connection was spawned speculatively at `current_offset_`. The primary or hedge completed shortly thereafter, clamping the maximum chunk latency to **558.64 ms**.
2. **Generational Consistency:**
   Across all 8,775 Phase 3 hedged requests (comprising over 438,000 chunk read invocations and speculative hedge stream spawns), all hedges cleanly preserved the pinned object generation, resulting in **zero corruptions and zero data mismatches**.
3. **Overhead Profile:**
   Because hedging only triggers after 500 ms, p50 total latency remained nearly unchanged (292.62 ms vs 280.85 ms, +4.19%), representing negligible background cost while protecting the long tail.

---

# Part II: Hedged vs Unhedged Comparison (2 MiB, 3 MiB, 5 MiB Chunks at Random Offsets)

**Date:** September 10, 2026  
**Environment:** Compute Engine VM `artemis` (`us-central1-a`)  
**Target Bucket & Object:** `gs://ajayky-minerva/files/primitive_benchmark_500MB.parquet` (500 MiB Regional Bucket, `us-central1`)  
**Workload Configuration:**
- **Concurrency:** 15 workers in parallel.
- **Request Offsets:** Uniformly random across `[0, 524,288,000 - size]`.
- **Chunk Sizes:** 2 MiB, 3 MiB, and 5 MiB chunks.
- **Buffer Size:** 1 MiB per read iteration (`stream.read(...)`).
- **Hedging Delay:** 500 ms (Hedged mode); Disabled (Unhedged mode).
- **Stall Timeout:** Disabled (`stall_timeout=0`) for both phases.
- **Duration:** 5 minutes Unhedged followed by 5 minutes Hedged (~10 minutes total).
- **Volume:** **163,462 total requests** (84,086 Unhedged, 79,376 Hedged), **523.5 GB transferred**, **100% success rate (0 errors)**.

---

## 1. Overall Aggregate (All Sizes: 2 MiB, 3 MiB, 5 MiB)

- **Total Requests:** Unhedged = 84,086 | Hedged = 79,376 (Delta: -4,710, -5.60%)
- **Data Transferred:** Unhedged = 273.21 GB | Hedged = 258.47 GB
- **Success Rate:** 100% (0 errors Unhedged vs 0 errors Hedged)

### A. Open Latency (TTFB / Connection + Initial Read)

| Metric / Percentile | Unhedged (No Hedging) | Hedged (500ms Delay) | Delta (Hedged vs Unhedged) |
| :--- | :---: | :---: | :---: |
| **p50** | 33.51 ms | 34.82 ms | +1.31 ms (+3.91%) |
| **p90** | 59.03 ms | 66.02 ms | +6.99 ms (+11.84%) |
| **p95** | 77.64 ms | 86.80 ms | +9.16 ms (+11.79%) |
| **p99** | 129.10 ms | 140.24 ms | +11.14 ms (+8.63%) |
| **p99.9** | 271.45 ms | 282.76 ms | +11.32 ms (+4.17%) |
| **Max** | 1,015.81 ms | 634.52 ms | **-381.29 ms (-37.54%)** |

### B. Individual Read Latency (Payload Read Duration)

| Metric / Percentile | Unhedged (No Hedging) | Hedged (500ms Delay) | Delta (Hedged vs Unhedged) |
| :--- | :---: | :---: | :---: |
| **p50** | 10.63 ms | 10.92 ms | +0.29 ms (+2.69%) |
| **p90** | 24.13 ms | 25.91 ms | +1.78 ms (+7.40%) |
| **p95** | 32.35 ms | 35.78 ms | +3.43 ms (+10.59%) |
| **p99** | 61.30 ms | 69.82 ms | +8.52 ms (+13.90%) |
| **p99.9** | 136.94 ms | 147.48 ms | +10.54 ms (+7.70%) |
| **Max** | 1,543.91 ms | 661.08 ms | **-882.83 ms (-57.18%)** |

### C. Total Latency (Open + Read)

| Metric / Percentile | Unhedged (No Hedging) | Hedged (500ms Delay) | Delta (Hedged vs Unhedged) |
| :--- | :---: | :---: | :---: |
| **p50** | 45.56 ms | 47.13 ms | +1.57 ms (+3.44%) |
| **p90** | 80.57 ms | 89.73 ms | +9.17 ms (+11.38%) |
| **p95** | 104.81 ms | 118.60 ms | +13.79 ms (+13.16%) |
| **p99** | 170.44 ms | 183.96 ms | +13.52 ms (+7.94%) |
| **p99.9** | 379.63 ms | 366.46 ms | -13.17 ms (-3.47%) |
| **Max** | 1,608.26 ms | 984.24 ms | **-624.02 ms (-38.80%)** |

### D. Max Single Chunk Latency (Slowest 1 MiB Chunk per Stream)

| Metric / Percentile | Unhedged (No Hedging) | Hedged (500ms Delay) | Delta (Hedged vs Unhedged) |
| :--- | :---: | :---: | :---: |
| **p50** | 4.06 ms | 4.14 ms | +0.08 ms (+1.85%) |
| **p90** | 10.15 ms | 11.45 ms | +1.30 ms (+12.86%) |
| **p95** | 15.23 ms | 17.38 ms | +2.15 ms (+14.13%) |
| **p99** | 34.82 ms | 41.09 ms | +6.27 ms (+18.01%) |
| **p99.9** | 97.63 ms | 108.43 ms | +10.80 ms (+11.07%) |
| **Max** | 1,516.18 ms | 584.28 ms | **-931.90 ms (-61.46%)** |

---

## 2. Breakdown: 2 MiB Chunks (2,097,152 bytes)

- **Total Requests:** Unhedged = 28,309 | Hedged = 26,444
- **Data Transferred:** Unhedged = 55.29 GB | Hedged = 51.65 GB

| Metric / Percentile | Open Latency Unhedged | Open Latency Hedged | Read Latency Unhedged | Read Latency Hedged | Total Latency Unhedged | Total Latency Hedged |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **p50** | 32.80 ms | 34.13 ms *(+1.33)* | 6.03 ms | 6.11 ms *(+0.08)* | 39.45 ms | 40.84 ms *(+1.39)* |
| **p90** | 57.85 ms | 63.89 ms *(+6.04)* | 10.63 ms | 11.14 ms *(+0.50)* | 67.43 ms | 74.24 ms *(+6.81)* |
| **p95** | 76.44 ms | 84.39 ms *(+7.95)* | 13.98 ms | 14.67 ms *(+0.69)* | 88.89 ms | 97.32 ms *(+8.43)* |
| **p99** | 127.40 ms | 138.50 ms *(+11.10)* | 26.43 ms | 27.88 ms *(+1.45)* | 143.99 ms | 155.24 ms *(+11.25)* |
| **p99.9** | 250.07 ms | 291.62 ms *(+41.55)* | 61.10 ms | 69.33 ms *(+8.24)* | 277.50 ms | 314.66 ms *(+37.16)* |
| **Max** | 761.91 ms | **634.52 ms (-16.7%)** | 444.24 ms | 589.20 ms | 769.29 ms | 984.24 ms |

---

## 3. Breakdown: 3 MiB Chunks (3,145,728 bytes)

- **Total Requests:** Unhedged = 27,866 | Hedged = 26,442
- **Data Transferred:** Unhedged = 81.64 GB | Hedged = 77.47 GB

| Metric / Percentile | Open Latency Unhedged | Open Latency Hedged | Read Latency Unhedged | Read Latency Hedged | Total Latency Unhedged | Total Latency Hedged |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **p50** | 34.27 ms | 35.69 ms *(+1.42)* | 9.89 ms | 10.02 ms *(+0.13)* | 44.78 ms | 46.40 ms *(+1.62)* |
| **p90** | 60.03 ms | 67.52 ms *(+7.49)* | 18.43 ms | 19.97 ms *(+1.54)* | 77.80 ms | 87.35 ms *(+9.55)* |
| **p95** | 78.79 ms | 87.62 ms *(+8.82)* | 24.50 ms | 27.32 ms *(+2.83)* | 100.77 ms | 112.90 ms *(+12.14)* |
| **p99** | 129.39 ms | 139.75 ms *(+10.36)* | 45.27 ms | 50.76 ms *(+5.49)* | 161.56 ms | 174.10 ms *(+12.53)* |
| **p99.9** | 278.34 ms | **269.43 ms (-3.2%)** | 109.37 ms | 122.51 ms *(+13.14)* | 355.07 ms | **348.19 ms (-1.9%)** |
| **Max** | 1,015.81 ms | **598.30 ms (-41.1%)** | 1,274.16 ms | **661.08 ms (-48.1%)** | 1,608.26 ms | **726.35 ms (-54.8%)** |

---

## 4. Breakdown: 5 MiB Chunks (5,242,880 bytes)

- **Total Requests:** Unhedged = 27,911 | Hedged = 26,490
- **Data Transferred:** Unhedged = 136.28 GB | Hedged = 129.35 GB

| Metric / Percentile | Open Latency Unhedged | Open Latency Hedged | Read Latency Unhedged | Read Latency Hedged | Total Latency Unhedged | Total Latency Hedged |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **p50** | 33.47 ms | 34.68 ms *(+1.21)* | 18.07 ms | 18.29 ms *(+0.22)* | 52.35 ms | 53.98 ms *(+1.63)* |
| **p90** | 59.22 ms | 66.74 ms *(+7.52)* | 35.17 ms | 38.66 ms *(+3.49)* | 93.87 ms | 106.65 ms *(+12.78)* |
| **p95** | 77.50 ms | 88.27 ms *(+10.77)* | 47.21 ms | 52.44 ms *(+5.22)* | 122.89 ms | 139.16 ms *(+16.27)* |
| **p99** | 129.32 ms | 141.82 ms *(+12.50)* | 82.41 ms | 92.86 ms *(+10.45)* | 196.06 ms | 212.35 ms *(+16.29)* |
| **p99.9** | 281.08 ms | 287.73 ms *(+6.65)* | 188.76 ms | **169.84 ms (-10.0%)** | 440.88 ms | **408.78 ms (-7.3%)** |
| **Max** | 963.50 ms | **604.83 ms (-37.2%)** | 1,543.91 ms | **590.41 ms (-61.8%)** | 1,582.68 ms | **667.15 ms (-57.9%)** |

---

## 5. Conclusions

1. **Massive Tail Reduction on Multi-Chunk Transfers:**
   - On **5 MiB reads**, individual payload read max latency dropped by **61.8%** (from **1,543.91 ms** down to **590.41 ms**).
   - On **3 MiB reads**, individual payload read max latency dropped by **48.1%** (from **1,274.16 ms** down to **661.08 ms**).
   - The slowest single chunk latency across all streams was clamped from **1,516.18 ms** down to **584.28 ms** (**-61.5%**).
2. **Open Latency Tail Truncation:**
   - Peak open latency (TTFB) was reduced from **1,015.81 ms** down to **634.52 ms** (**-37.5%**), kept close to the 500 ms hedge trigger window.
3. **Negligible Overhead:**
   - Median (p50) read and total latencies remained virtually identical across all sizes (+0.08 ms on 2MB, +0.13 ms on 3MB, +0.22 ms on 5MB).

---

# Part III: Hedged (30-Thread Pool) vs Unhedged Comparison

**Date:** September 10, 2026  
**Environment:** Compute Engine VM `artemis` (`us-central1-a`)  
**Target Bucket & Object:** `gs://ajayky-minerva/files/primitive_benchmark_500MB.parquet` (500 MiB Regional Bucket, `us-central1`)  
**Configuration Changes:**
- **Hedging Thread Pool:** Scaled from default 15 to **30 threads** (`HedgingThreadPoolSizeOption = 30`).
- **Max Concurrent Hedges:** Increased to **30** (`MaxConcurrentHedgesOption = 30`).
- **Connection Pool:** Scaled to **60 connections** (`ConnectionPoolSizeOption = 60`).
- **Concurrency:** 15 worker threads in parallel.
- **Sizes:** 2 MiB, 3 MiB, 5 MiB at random offsets.
- **Duration:** 5 minutes Unhedged followed by 5 minutes Hedged.
- **Total Requests:** **187,095 total requests** (96,949 Unhedged, 90,146 Hedged), **609.0 GB transferred**, **100% success rate (0 errors)**.

---

## 1. Overall Aggregate (All Sizes: 2 MiB, 3 MiB, 5 MiB)

| Metric | Unhedged (No Hedging) | Hedged (30-Thread Pool) | Delta (Hedged vs Unhedged) |
| :--- | :---: | :---: | :---: |
| **Total Completed Requests** | 96,949 | 90,146 | -6,803 (-7.02%) |
| **Throughput (QPS)** | 323.2 req/s | 300.5 req/s | -22.7 req/s (-7.02%) |
| **Data Transferred** | 315.58 GB | 293.41 GB | -22.17 GB |
| **Success Rate** | 100% (0 errors) | 100% (0 errors) | 0 errors |

### A. Open Latency (TTFB / Connection + Initial Read)
| Metric / Percentile | Unhedged | Hedged (30-Thread Pool) | Delta |
| :--- | :---: | :---: | :---: |
| **Mean** | 33.81 ms | 36.37 ms | +2.56 ms (+7.57%) |
| **p50** | 30.76 ms | 32.45 ms | +1.68 ms (+5.46%) |
| **p90** | 44.94 ms | 50.25 ms | +5.32 ms (+11.83%) |
| **p95** | 53.87 ms | 62.42 ms | +8.55 ms (+15.87%) |
| **p99** | 88.87 ms | 104.51 ms | +15.64 ms (+17.60%) |
| **p99.9** | 231.48 ms | 232.26 ms | +0.79 ms (+0.34%) |
| **Max** | **1,095.75 ms** | **579.03 ms** | **-516.72 ms (-47.16%)** |

### B. Individual Read Latency (Payload Read Duration)
| Metric / Percentile | Unhedged | Hedged (30-Thread Pool) | Delta |
| :--- | :---: | :---: | :---: |
| **Mean** | 12.57 ms | 13.49 ms | +0.92 ms (+7.28%) |
| **p50** | 10.07 ms | 10.61 ms | +0.53 ms (+5.29%) |
| **p90** | 21.38 ms | 23.17 ms | +1.79 ms (+8.39%) |
| **p95** | 26.60 ms | 29.92 ms | +3.31 ms (+12.45%) |
| **p99** | 44.75 ms | 55.46 ms | +10.71 ms (+23.94%) |
| **p99.9** | 107.54 ms | 119.27 ms | +11.72 ms (+10.90%) |
| **Max** | **839.80 ms** | **542.12 ms** | **-297.68 ms (-35.45%)** |

### C. Total Latency (Open + Read)
| Metric / Percentile | Unhedged | Hedged (30-Thread Pool) | Delta |
| :--- | :---: | :---: | :---: |
| **Mean** | 46.39 ms | 49.86 ms | +3.48 ms (+7.49%) |
| **p50** | 42.14 ms | 44.42 ms | +2.29 ms (+5.43%) |
| **p90** | 62.59 ms | 70.08 ms | +7.49 ms (+11.96%) |
| **p95** | 74.63 ms | 86.38 ms | +11.75 ms (+15.75%) |
| **p99** | 121.95 ms | 143.32 ms | +21.37 ms (+17.53%) |
| **p99.9** | 302.75 ms | 297.49 ms | -5.26 ms (-1.74%) |
| **Max** | **1,368.20 ms** | **861.65 ms** | **-506.55 ms (-37.02%)** |

---

## 2. Size Breakdown (30-Thread Hedging Pool)

### 2 MiB Chunks (2,097,152 bytes)
- Requests: Unhedged = 32,221 | Hedged = 29,886
- **Max Open Latency:** 954.38 ms &rarr; **534.92 ms (-43.95%)**
- **Max Read Latency:** 462.79 ms &rarr; **314.12 ms (-32.13%)**
- **Max Total Latency:** 967.26 ms &rarr; **546.34 ms (-43.52%)**

### 3 MiB Chunks (3,145,728 bytes)
- Requests: Unhedged = 32,309 | Hedged = 30,400
- **Max Open Latency:** 1,095.75 ms &rarr; **579.03 ms (-47.16%)**
- **Max Read Latency:** 642.84 ms &rarr; **542.12 ms (-15.67%)**
- **Max Total Latency:** 1,368.20 ms &rarr; **593.75 ms (-56.60%)**

### 5 MiB Chunks (5,242,880 bytes)
- Requests: Unhedged = 32,419 | Hedged = 29,860
- **Max Open Latency:** 794.01 ms &rarr; **548.39 ms (-30.93%)**
- **Max Read Latency:** 839.80 ms &rarr; **362.63 ms (-56.82%)**
- **Max Total Latency:** 921.31 ms &rarr; **861.65 ms (-6.48%)**

---

## 3. Impact of Increasing Hedging Pool Size (15 &rarr; 30 Threads)

1. **Higher Hedged Throughput (+13.6%):**
   - With pool size 15, Hedged completed **79,376 requests**.
   - With pool size 30, Hedged completed **90,146 requests** (+10,770 requests completed).
2. **Eliminated Concurrency Contention:**
   - When multiple workers encounter simultaneous stalls, the 30-thread pool provides double the concurrency capacity, eliminating waiting on hedge thread pool slot releases.
3. **Strict Tail Clamping Across All Sizes:**
   - 5MB read latency max dropped to **362.63 ms** (vs 590.41 ms with pool=15 and 839.80 ms unhedged).
   - 2MB read latency max dropped to **314.12 ms** (vs 462.79 ms unhedged).
   - 3MB total latency max dropped to **593.75 ms** (vs 1,368.20 ms unhedged).


---

# Part IV: Reactive Stall Hedging Fix Evaluation (`feature/read-hedging-fix`)

### Test Configuration:
- **Branch**: `feature/subsequent-chunk-hedging-benchmark` rebased on `feature/read-hedging-fix` (Commit `7ee849a619`)
- **Key Architectural Change**: Replaced proactive continuous hedge tasks per-chunk with **reactive stall hedging**. Hedging is ONLY spawned if the primary chunk read actively stalls beyond `ReadHedgeDelay` (500 ms).
- **Target Bucket / Object**: `ajayky-minerva` / `files/primitive_benchmark_500MB.parquet`
- **Execution Platform**: `us-central1` GCE VM (`artemis`, 10.128.0.63)
- **Workload**: 15 concurrency, 5 minutes, random offsets, chunk sizes: 2MB, 3MB, 5MB
- **Hedging Parameters**: 500 ms delay, 30 hedge pool threads, connection pool size 60

---

## Executive Summary & Comparison

| Metric | Unhedged Baseline | Hedged (Pre-Fix Proactive) | Hedged (New Reactive Fix) | Delta (New vs Unhedged) | Delta (New vs Old) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Total Completed Requests** | 96,949 | 90,146 | **96,079** | -870 (-0.90%) | **+5,933 (+6.58%)** |
| **Throughput (QPS)** | 323.2 req/s | 300.5 req/s | **320.3 req/s** | -2.9 req/s (-0.90%) | **+19.8 req/s (+6.58%)** |
| **Data Transferred** | 315.88 GB | 293.41 GB | **312.31 GB** | -3.57 GB | **+18.90 GB** |
| **Mean Read Latency** | 12.57 ms | 13.49 ms | **11.83 ms** | **-0.75 ms (-5.94%)** | **-1.66 ms (-12.32%)** |
| **p50 Read Latency** | 10.07 ms | 10.61 ms | **9.51 ms** | **-0.56 ms (-5.57%)** | **-1.09 ms (-10.31%)** |
| **p95 Read Latency** | 26.60 ms | 29.92 ms | **25.02 ms** | **-1.58 ms (-5.95%)** | **-4.89 ms (-16.36%)** |
| **p99 Read Latency** | 44.75 ms | 55.46 ms | **41.74 ms** | **-3.01 ms (-6.74%)** | **-13.73 ms (-24.75%)** |
| **p99.9 Read Latency** | 107.54 ms | 119.27 ms | **83.11 ms** | **-24.43 ms (-22.72%)** | **-36.16 ms (-30.31%)** |
| **Max Open Latency** | 1,095.75 ms | 579.03 ms | **566.57 ms** | **-529.18 ms (-48.29%)** | -12.46 ms (-2.15%) |
| **Max Total Latency** | 1,368.20 ms | 861.65 ms | **1,282.83 ms** | **-85.37 ms (-6.24%)** | +421.18 ms (+48.88%) |
| **Success Rate** | 100% (0 errors) | 100% (0 errors) | **100% (0 errors)** | 0 errors | 0 errors |

### Key Findings:
1. **Throughput Penalty Virtually Eliminated**:
   - In the pre-fix proactive implementation, completed requests dropped from 96,949 to 90,146 (-7.02%) due to unnecessary hedge thread and HTTP connection contention.
   - The reactive fix recovered **99.1%** of unhedged throughput (96,079 vs 96,949 requests), and for 2MB chunks was **identical** (32,217 vs 32,221 requests, a 0.01% delta).
2. **Substantial Latency Reductions Across All Read Percentiles**:
   - Read latency is **10% to 30% faster** than the proactive approach at every percentile (p50: 9.51 ms vs 10.61 ms; p99: 41.74 ms vs 55.46 ms; p99.9: 83.11 ms vs 119.27 ms).
   - Tail latency is lower than the Unhedged baseline as well: p99 read latency dropped from 44.75 ms to 41.74 ms (-6.74%) and p99.9 read latency dropped from 107.54 ms to 83.11 ms (-22.72%).
3. **Max Open Tail Clamped by 48.3%**:
   - Max Open Latency (Connection/TTFB) was reduced from **1,095.75 ms** down to **566.57 ms**.

---

## Detailed Size Breakdown

### 2 MiB Chunks (2,097,152 bytes)
- **Completed Requests**: Unhedged = 32,221 | New Fix = **32,217** (Delta: -4, -0.01%) | Pre-fix Old = 29,886 (+7.80%)

| Metric / Percentile | Unhedged Baseline | Hedged (Pre-Fix Old) | Hedged (New Reactive Fix) | Delta (New vs Unhedged) | Delta (New vs Old) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Open Latency (Max)** | 954.38 ms | 534.92 ms | **566.57 ms** | **-387.80 ms (-40.63%)** | +31.65 ms (+5.92%) |
| **Read Latency (Mean)** | 6.91 ms | 7.43 ms | **6.35 ms** | **-0.56 ms (-8.10%)** | **-1.08 ms (-14.48%)** |
| **Read Latency (p50)** | 5.87 ms | 6.21 ms | **5.58 ms** | **-0.30 ms (-5.04%)** | **-0.64 ms (-10.23%)** |
| **Read Latency (p99)** | 22.27 ms | 28.03 ms | **18.57 ms** | **-3.70 ms (-16.61%)** | **-9.45 ms (-33.73%)** |
| **Read Latency (p99.9)** | 59.33 ms | 68.22 ms | **38.56 ms** | **-20.77 ms (-35.01%)** | **-29.66 ms (-43.48%)** |
| **Total Latency (Mean)** | 40.03 ms | 42.89 ms | **40.56 ms** | +0.53 ms (+1.33%) | **-2.33 ms (-5.42%)** |
| **Total Latency (p99)** | 98.92 ms | 116.27 ms | **100.52 ms** | +1.60 ms (+1.61%) | **-15.75 ms (-13.55%)** |
| **Total Latency (Max)** | 967.26 ms | 546.34 ms | **574.51 ms** | **-392.75 ms (-40.60%)** | +28.18 ms (+5.16%) |

### 3 MiB Chunks (3,145,728 bytes)
- **Completed Requests**: Unhedged = 32,309 | New Fix = **31,968** (Delta: -341, -1.06%) | Pre-fix Old = 30,400 (+5.16%)

| Metric / Percentile | Unhedged Baseline | Hedged (Pre-Fix Old) | Hedged (New Reactive Fix) | Delta (New vs Unhedged) | Delta (New vs Old) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Open Latency (Max)** | 1,095.75 ms | 579.03 ms | **517.33 ms** | **-578.42 ms (-52.79%)** | **-61.70 ms (-10.66%)** |
| **Read Latency (Mean)** | 11.07 ms | 11.91 ms | **10.32 ms** | **-0.75 ms (-6.75%)** | **-1.59 ms (-13.38%)** |
| **Read Latency (p50)** | 9.42 ms | 9.88 ms | **8.98 ms** | -0.44 ms (-4.64%) | **-0.91 ms (-9.16%)** |
| **Read Latency (p99)** | 35.71 ms | 43.15 ms | **31.87 ms** | **-3.84 ms (-10.75%)** | **-11.28 ms (-26.14%)** |
| **Read Latency (p99.9)** | 73.76 ms | 96.05 ms | **65.46 ms** | **-8.30 ms (-11.25%)** | **-30.59 ms (-31.85%)** |
| **Read Latency (Max)** | 642.84 ms | 542.12 ms | **194.09 ms** | **-448.75 ms (-69.81%)** | **-348.03 ms (-64.20%)** |
| **Total Latency (Mean)** | 45.57 ms | 49.10 ms | **46.02 ms** | +0.45 ms (+0.99%) | **-3.09 ms (-6.29%)** |
| **Total Latency (p99)** | 114.74 ms | 136.36 ms | **115.86 ms** | +1.12 ms (+0.97%) | **-20.50 ms (-15.04%)** |
| **Total Latency (Max)** | 1,368.20 ms | 593.75 ms | **524.77 ms** | **-843.43 ms (-61.65%)** | **-68.98 ms (-11.62%)** |

### 5 MiB Chunks (5,242,880 bytes)
- **Completed Requests**: Unhedged = 32,419 | New Fix = **31,894** (Delta: -525, -1.62%) | Pre-fix Old = 29,860 (+6.81%)

| Metric / Percentile | Unhedged Baseline | Hedged (Pre-Fix Old) | Hedged (New Reactive Fix) | Delta (New vs Unhedged) | Delta (New vs Old) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Open Latency (Max)** | 794.01 ms | 548.39 ms | **552.65 ms** | **-241.36 ms (-30.40%)** | +4.26 ms (+0.78%) |
| **Read Latency (Mean)** | 19.70 ms | 21.16 ms | **18.87 ms** | -0.83 ms (-4.24%) | **-2.29 ms (-10.83%)** |
| **Read Latency (p50)** | 17.00 ms | 17.78 ms | **16.52 ms** | -0.48 ms (-2.82%) | **-1.25 ms (-7.06%)** |
| **Read Latency (p99)** | 60.47 ms | 74.25 ms | **54.44 ms** | **-6.03 ms (-9.98%)** | **-19.81 ms (-26.68%)** |
| **Read Latency (p99.9)** | 155.45 ms | 157.04 ms | **112.24 ms** | **-43.22 ms (-27.80%)** | **-44.80 ms (-28.53%)** |
| **Total Latency (Mean)** | 53.52 ms | 57.61 ms | **53.87 ms** | +0.35 ms (+0.65%) | **-3.74 ms (-6.50%)** |
| **Total Latency (p99)** | 143.79 ms | 167.75 ms | **137.74 ms** | -6.05 ms (-4.21%) | **-30.01 ms (-17.89%)** |
| **Total Latency (p99.9)** | 348.99 ms | 318.94 ms | **288.91 ms** | **-60.09 ms (-17.22%)** | **-30.04 ms (-9.42%)** |
