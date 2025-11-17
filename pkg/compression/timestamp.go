package compression

import (
	"bytes"
	"fmt"
	"io"
)

// TimestampEncoder implements delta-of-delta encoding for timestamps as described
// in Facebook's Gorilla paper. This achieves excellent compression for regularly-spaced
// time series data.
//
// Algorithm:
// - Store first timestamp as-is (64 bits)
// - Store first delta (difference from first timestamp)
// - For subsequent values, encode delta-of-delta using variable-length encoding:
//   - If delta-of-delta is 0: write 1 bit (0)
//   - If delta-of-delta fits in [-63, 64]: write 2 control bits (10) + 7 bits
//   - If delta-of-delta fits in [-255, 256]: write 3 control bits (110) + 9 bits
//   - If delta-of-delta fits in [-2047, 2048]: write 4 control bits (1110) + 12 bits
//   - Otherwise: write 4 control bits (1111) + 32 bits
type TimestampEncoder struct {
	bw         *BitWriter
	t0         int64 // First timestamp
	t1         int64 // Previous timestamp
	delta      int64 // Previous delta (t1 - t0)
	count      int   // Number of timestamps encoded
	headerSize int   // Size of header in bits
}

// NewTimestampEncoder creates a new timestamp encoder
func NewTimestampEncoder() *TimestampEncoder {
	buf := &bytes.Buffer{}
	return &TimestampEncoder{
		bw: NewBitWriter(buf),
	}
}

// Encode encodes a timestamp using delta-of-delta compression
func (e *TimestampEncoder) Encode(t int64) error {
	if e.count == 0 {
		// First timestamp: store as-is (64 bits)
		e.t0 = t
		e.t1 = t
		e.count++
		return e.bw.WriteBits(uint64(t), 64)
	}

	if e.count == 1 {
		// Second timestamp: store delta (t - t0)
		e.delta = t - e.t1
		e.t1 = t
		e.count++

		// Store delta as signed 64-bit value
		return e.bw.WriteBits(uint64(e.delta), 64)
	}

	// Subsequent timestamps: delta-of-delta encoding
	delta := t - e.t1
	dod := delta - e.delta

	// Update state
	e.delta = delta
	e.t1 = t
	e.count++

	// Variable-length encoding based on delta-of-delta magnitude
	if dod == 0 {
		// Delta hasn't changed: 1 bit (0)
		return e.bw.WriteBit(0)
	}

	if dod >= -63 && dod <= 64 {
		// Small change: 2 control bits (10) + 7 bits
		if err := e.bw.WriteBits(0b10, 2); err != nil {
			return err
		}
		return e.bw.WriteBits(uint64(dod)&0x7F, 7)
	}

	if dod >= -255 && dod <= 256 {
		// Medium change: 3 control bits (110) + 9 bits
		if err := e.bw.WriteBits(0b110, 3); err != nil {
			return err
		}
		return e.bw.WriteBits(uint64(dod)&0x1FF, 9)
	}

	if dod >= -2047 && dod <= 2048 {
		// Large change: 4 control bits (1110) + 12 bits
		if err := e.bw.WriteBits(0b1110, 4); err != nil {
			return err
		}
		return e.bw.WriteBits(uint64(dod)&0xFFF, 12)
	}

	// Very large change: 4 control bits (1111) + 32 bits
	if err := e.bw.WriteBits(0b1111, 4); err != nil {
		return err
	}
	return e.bw.WriteBits(uint64(dod)&0xFFFFFFFF, 32)
}

// Finish finalizes the encoding and returns the compressed bytes
func (e *TimestampEncoder) Finish() ([]byte, error) {
	if err := e.bw.Flush(); err != nil {
		return nil, err
	}

	// Extract bytes from buffer
	if buf, ok := e.bw.w.(*bytes.Buffer); ok {
		return buf.Bytes(), nil
	}

	return nil, fmt.Errorf("unexpected writer type")
}

// Count returns the number of timestamps encoded
func (e *TimestampEncoder) Count() int {
	return e.count
}

// BitsWritten returns the total bits written
func (e *TimestampEncoder) BitsWritten() uint64 {
	return e.bw.BitsWritten()
}

// TimestampDecoder implements delta-of-delta decoding for timestamps
type TimestampDecoder struct {
	br    *BitReader
	t0    int64 // First timestamp
	t1    int64 // Previous timestamp
	delta int64 // Previous delta
	count int   // Number of timestamps decoded
}

// NewTimestampDecoder creates a new timestamp decoder
func NewTimestampDecoder(data []byte) *TimestampDecoder {
	return &TimestampDecoder{
		br: NewBitReader(data),
	}
}

// Decode decodes the next timestamp
func (d *TimestampDecoder) Decode() (int64, error) {
	if d.count == 0 {
		// First timestamp: read 64 bits
		val, err := d.br.ReadBits(64)
		if err != nil {
			return 0, err
		}
		d.t0 = int64(val)
		d.t1 = d.t0
		d.count++
		return d.t0, nil
	}

	if d.count == 1 {
		// Second timestamp: read delta (64 bits)
		val, err := d.br.ReadBits(64)
		if err != nil {
			return 0, err
		}
		d.delta = int64(val)
		d.t1 = d.t0 + d.delta
		d.count++
		return d.t1, nil
	}

	// Subsequent timestamps: delta-of-delta decoding
	// Read control bits to determine encoding
	bit, err := d.br.ReadBit()
	if err != nil {
		return 0, err
	}

	var dod int64

	if bit == 0 {
		// Delta hasn't changed: dod = 0
		dod = 0
	} else {
		// Read more control bits
		bit2, err := d.br.ReadBit()
		if err != nil {
			return 0, err
		}

		if bit2 == 0 {
			// 10: read 7 bits
			val, err := d.br.ReadBits(7)
			if err != nil {
				return 0, err
			}
			// Sign extend from 7 bits
			dod = int64(val)
			if dod > 64 {
				dod = dod - 128
			}
		} else {
			// Read third control bit
			bit3, err := d.br.ReadBit()
			if err != nil {
				return 0, err
			}

			if bit3 == 0 {
				// 110: read 9 bits
				val, err := d.br.ReadBits(9)
				if err != nil {
					return 0, err
				}
				// Sign extend from 9 bits
				dod = int64(val)
				if dod > 256 {
					dod = dod - 512
				}
			} else {
				// Read fourth control bit
				bit4, err := d.br.ReadBit()
				if err != nil {
					return 0, err
				}

				if bit4 == 0 {
					// 1110: read 12 bits
					val, err := d.br.ReadBits(12)
					if err != nil {
						return 0, err
					}
					// Sign extend from 12 bits
					dod = int64(val)
					if dod > 2048 {
						dod = dod - 4096
					}
				} else {
					// 1111: read 32 bits
					val, err := d.br.ReadBits(32)
					if err != nil {
						return 0, err
					}
					// Sign extend from 32 bits
					dod = int64(int32(val))
				}
			}
		}
	}

	// Compute timestamp
	d.delta = d.delta + dod
	d.t1 = d.t1 + d.delta
	d.count++

	return d.t1, nil
}

// DecodeAll decodes all timestamps and returns them as a slice
func (d *TimestampDecoder) DecodeAll(count int) ([]int64, error) {
	timestamps := make([]int64, 0, count)

	for i := 0; i < count; i++ {
		t, err := d.Decode()
		if err != nil {
			if err == io.EOF && i == count {
				break
			}
			return nil, fmt.Errorf("failed to decode timestamp %d: %w", i, err)
		}
		timestamps = append(timestamps, t)
	}

	return timestamps, nil
}

// Count returns the number of timestamps decoded
func (d *TimestampDecoder) Count() int {
	return d.count
}
