# Compression Algorithms

This document explains the compression algorithms used in the Time-Series Database, inspired by Facebook's Gorilla paper.

## Overview

Our TSDB achieves **20-30x compression ratios** for time-series data using two complementary algorithms:

1. **Delta-of-delta encoding** for timestamps
2. **XOR compression** for float64 values

These algorithms are specifically designed for time-series data characteristics:
- Timestamps are often regularly spaced (e.g., every 60 seconds)
- Values change slowly over time (e.g., CPU usage varies gradually)

## Delta-of-Delta Timestamp Compression

### Motivation

Time-series data typically has regularly-spaced timestamps. For example:
```
1640000000000 (2021-12-20 12:00:00)
1640000060000 (2021-12-20 12:01:00)
1640000120000 (2021-12-20 12:02:00)
```

The delta (difference) between consecutive timestamps is constant:
```
60000, 60000, 60000, ...
```

The delta-of-delta (difference of differences) is zero:
```
0, 0, 0, ...
```

We can compress this very efficiently!

### Algorithm

1. **First timestamp** (T₀): Store as-is (64 bits)
   ```
   T₀ = 1640000000000
   ```

2. **Second timestamp** (T₁): Store delta (64 bits)
   ```
   Δ = T₁ - T₀ = 60000
   ```

3. **Subsequent timestamps**: Store delta-of-delta using variable-length encoding

   ```
   Current timestamp: Tₙ
   Previous timestamp: Tₙ₋₁
   Previous delta: Δₙ₋₁

   Current delta: Δₙ = Tₙ - Tₙ₋₁
   Delta-of-delta: ΔΔ = Δₙ - Δₙ₋₁
   ```

   **Encoding rules**:
   - `ΔΔ = 0`: Write **1 bit** → `0`
   - `ΔΔ ∈ [-63, 64]`: Write **9 bits** → `10` + 7 bits
   - `ΔΔ ∈ [-255, 256]`: Write **12 bits** → `110` + 9 bits
   - `ΔΔ ∈ [-2047, 2048]`: Write **16 bits** → `1110` + 12 bits
   - Otherwise: Write **36 bits** → `1111` + 32 bits

### Example

Compressing timestamps with 60-second intervals:

```
Timestamps:  1000, 2000, 3000, 4000, 5000
Deltas:           1000, 1000, 1000, 1000
Delta-of-delta:        0,    0,    0
```

**Storage**:
- T₀: 64 bits (1000)
- Δ₀: 64 bits (1000)
- ΔΔ₁: 1 bit (0)
- ΔΔ₂: 1 bit (0)
- ΔΔ₃: 1 bit (0)

**Total**: 131 bits vs 320 bits uncompressed = **2.4x compression**

For longer sequences with regular intervals, compression can exceed **40x**!

### Implementation

```go
// Encoding
encoder := compression.NewTimestampEncoder()
for _, ts := range timestamps {
    encoder.Encode(ts)
}
compressed, _ := encoder.Finish()

// Decoding
decoder := compression.NewTimestampDecoder(compressed)
timestamps, _ := decoder.DecodeAll(count)
```

## XOR Value Compression

### Motivation

Time-series values often change slowly. For example, CPU usage:
```
0.750000
0.751234
0.752100
0.751800
```

When we XOR consecutive values, many bits are identical:
```
0.750000 = 0x3FE8000000000000
0.751234 = 0x3FE804F40FC52B05
         XOR 0x00000000004F40FC5
```

The XOR has many **leading zeros** and **trailing zeros**. We only need to store the significant middle bits!

### Algorithm

1. **First value** (V₀): Store as-is (64 bits)
   ```
   V₀ = 0.75 = 0x3FE8000000000000
   ```

2. **Subsequent values**: XOR with previous value

   ```
   Current value: Vₙ
   Previous value: Vₙ₋₁
   XOR: X = Vₙ ⊕ Vₙ₋₁
   ```

   **Encoding rules**:

   a) **If X = 0** (value unchanged):
      - Write **1 bit** → `0`

   b) **If leading/trailing zeros match previous**:
      - Write **2 bits** → `10`
      - Write significant bits only

   c) **Otherwise**:
      - Write **2 bits** → `11`
      - Write **5 bits**: leading zeros count
      - Write **6 bits**: block size (number of significant bits)
      - Write significant bits

### Example

Compressing slowly changing values:

```
Values: 0.75, 0.75, 0.751, 0.750
```

**Storage**:
- V₀: 64 bits (0.75)
- V₁: 1 bit (0) - same value
- V₂: ~14 bits (11 + 5 + 6 + significant bits)
- V₃: ~2 bits (10 + reuse previous block)

**Typical compression**: 8-16x for slowly changing metrics

### Implementation

```go
// Encoding
encoder := compression.NewValueEncoder()
for _, val := range values {
    encoder.Encode(val)
}
compressed, _ := encoder.Finish()

// Decoding
decoder := compression.NewValueDecoder(compressed)
values, _ := decoder.DecodeAll(count)
```

## Chunk Format

Chunks combine compressed timestamps and values:

```
┌─────────────────────────────────────┐
│ Header (24 bytes)                   │
├─────────────────────────────────────┤
│ - minTime (8 bytes)                 │
│ - maxTime (8 bytes)                 │
│ - numSamples (2 bytes)              │
│ - dataLength (4 bytes)              │
│ - encoding (2 bytes)                │
├─────────────────────────────────────┤
│ Data (variable)                     │
├─────────────────────────────────────┤
│ - timestampLength (4 bytes)         │
│ - compressed timestamps (N bytes)   │
│ - compressed values (M bytes)       │
├─────────────────────────────────────┤
│ Footer (4 bytes)                    │
├─────────────────────────────────────┤
│ - CRC32 checksum (4 bytes)          │
└─────────────────────────────────────┘
```

### Chunk Properties

- **Default size**: 120 samples (~2 hours @ 1-minute intervals)
- **Checksummed**: CRC32 protects against corruption
- **Self-contained**: Includes metadata for time-range queries

## Block Format

Blocks organize chunks on disk:

```
data/
├── 01H8XABC00000000/          # Block ULID (time-sortable)
│   ├── meta.json              # Block metadata
│   ├── chunks/                # Compressed chunks
│   │   ├── 000001
│   │   ├── 000002
│   │   └── ...
│   └── index                  # Series index (Phase 4)
```

### Block Metadata (meta.json)

```json
{
  "ulid": "01H8XABC00000000",
  "minTime": 1640000000000,
  "maxTime": 1640007200000,
  "stats": {
    "numSamples": 1000000,
    "numSeries": 5000,
    "numChunks": 10000
  },
  "version": 1,
  "seriesChunks": {
    "12345": 1,
    "67890": 2
  }
}
```

## Performance Characteristics

### Compression Ratios

Based on our test suite:

| Data Type | Uncompressed | Compressed | Ratio |
|-----------|-------------|------------|-------|
| Regular timestamps (360 samples) | 2,880 bytes | 61 bytes | **47x** |
| Slowly changing values (360 samples) | 2,880 bytes | 94 bytes | **31x** |
| Combined (120 samples) | 1,920 bytes | 66 bytes | **29x** |

**Real-world average**: 20-30x compression for typical monitoring metrics

### Speed

Benchmarks on modern hardware:

| Operation | Throughput |
|-----------|------------|
| Timestamp encoding | 1-2M samples/sec |
| Timestamp decoding | 3-5M samples/sec |
| Value encoding | 800K-1.5M samples/sec |
| Value decoding | 2-4M samples/sec |
| Chunk creation | 100K-200K samples/sec |
| Chunk iteration | 200K-400K samples/sec |

### Memory Efficiency

- **Encoding**: Minimal buffering, streaming-friendly
- **Decoding**: Zero-copy where possible
- **Chunk overhead**: 28 bytes per chunk (header + footer)

## Comparison with Other Systems

| System | Compression | Ratio | Speed |
|--------|-------------|-------|-------|
| **Our TSDB** | Delta-of-delta + XOR | 20-30x | Very Fast |
| **Prometheus** | Similar (Gorilla-based) | 15-25x | Fast |
| **InfluxDB** | Snappy + Delta encoding | 10-20x | Fast |
| **VictoriaMetrics** | Custom | 25-40x | Very Fast |
| **Gzip** | General-purpose | 5-10x | Slow |

## Best Practices

### For Maximum Compression

1. **Use regular intervals**: Enables better delta-of-delta compression
   ```go
   // Good: Regular 60-second intervals
   samples := []series.Sample{
       {Timestamp: 1000, Value: 0.5},
       {Timestamp: 2000, Value: 0.6},
       {Timestamp: 3000, Value: 0.7},
   }
   ```

2. **Keep values changing slowly**: XOR compression works best for gradual changes
   ```go
   // Good: Gradual changes
   values := []float64{0.750, 0.751, 0.752, 0.753}

   // Bad: Random jumps
   values := []float64{0.5, 10.3, 2.1, 99.8}
   ```

3. **Group similar series**: Chunks compress better when values have similar patterns

### For Maximum Performance

1. **Batch inserts**: Reduce per-sample overhead
   ```go
   // Good: Batch insert
   db.Insert(series, samples)

   // Bad: Individual inserts
   for _, sample := range samples {
       db.Insert(series, []series.Sample{sample})
   }
   ```

2. **Use appropriate chunk sizes**: Default 120 samples is optimal for most cases

3. **Enable write-ahead log**: Protects against data loss with minimal overhead

## References

- [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf) - Facebook's original paper
- [Prometheus TSDB Format](https://github.com/prometheus/prometheus/tree/main/tsdb/docs/format) - Similar implementation
- [Time-Series Compression Algorithms](https://www.timescale.com/blog/time-series-compression-algorithms-explained/) - Comparison of approaches

## Implementation Details

### Bit-Level I/O

Our implementation uses custom `BitWriter` and `BitReader` classes for efficient bit-level operations:

```go
// Writing bits
bw := compression.NewBitWriter(buffer)
bw.WriteBit(1)
bw.WriteBits(0b101, 3)  // Write 3 bits
bw.Flush()

// Reading bits
br := compression.NewBitReader(data)
bit, _ := br.ReadBit()
value, _ := br.ReadBits(3)  // Read 3 bits
```

### Optimization Techniques

1. **Bit manipulation**: Fast bit counting using bitwise operations
2. **Buffer pooling**: Reuse buffers to reduce allocations
3. **Streaming**: Process data incrementally, avoid loading entire dataset
4. **SIMD-ready**: Algorithm structure allows for future vectorization

## Future Improvements

1. **Dictionary compression**: For repeated label values
2. **SIMD acceleration**: Vectorize bit operations
3. **Adaptive encoding**: Choose encoding based on data characteristics
4. **Multi-resolution**: Store downsampled data for long-range queries

---

**Built with ❤️ based on the Gorilla paper**
