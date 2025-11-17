package compression

import (
	"fmt"
	"math"
)

// TimestampEncoder implements Gorilla timestamp compression using delta-of-delta encoding.
// Based on the Facebook Gorilla paper: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
//
// Encoding scheme:
// - First timestamp: stored as full 64-bit value
// - First delta: stored as full 64-bit value
// - Subsequent deltas: delta-of-delta with variable-length encoding:
//   - 0: 1 bit (0)
//   - [-63, 64]: 2 bits (10) + 7 bits
//   - [-255, 256]: 3 bits (110) + 9 bits
//   - [-2047, 2048]: 4 bits (1110) + 12 bits
//   - Otherwise: 4 bits (1111) + 32 bits
type TimestampEncoder struct {
	w         *BitWriter
	tPrev     int64 // Previous timestamp
	tDelta    int64 // Previous delta
	count     int   // Number of timestamps encoded
	finalized bool  // Whether Finish() has been called
}

// NewTimestampEncoder creates a new timestamp encoder.
func NewTimestampEncoder() *TimestampEncoder {
	return &TimestampEncoder{
		w: NewBitWriter(128),
	}
}

// Encode adds a timestamp to the encoder.
func (e *TimestampEncoder) Encode(t int64) error {
	if e.finalized {
		return fmt.Errorf("encoder already finalized")
	}

	if e.count == 0 {
		// First timestamp: store as-is
		e.tPrev = t
		e.w.WriteUint64(uint64(t))
		e.count++
		return nil
	}

	if e.count == 1 {
		// Second timestamp: store delta
		e.tDelta = t - e.tPrev
		e.tPrev = t
		e.w.WriteUint64(uint64(e.tDelta))
		e.count++
		return nil
	}

	// Compute delta and delta-of-delta
	newDelta := t - e.tPrev
	deltaOfDelta := newDelta - e.tDelta

	// Update state for next iteration
	e.tPrev = t
	e.tDelta = newDelta

	// Encode delta-of-delta using variable-length encoding
	if deltaOfDelta == 0 {
		// '0' - 1 bit
		e.w.WriteBit(0)
	} else if deltaOfDelta >= -63 && deltaOfDelta <= 64 {
		// '10' + 7 bits
		e.w.WriteBits(0b10, 2)
		e.w.WriteBits(uint64(deltaOfDelta)&0x7F, 7)
	} else if deltaOfDelta >= -255 && deltaOfDelta <= 256 {
		// '110' + 9 bits
		e.w.WriteBits(0b110, 3)
		e.w.WriteBits(uint64(deltaOfDelta)&0x1FF, 9)
	} else if deltaOfDelta >= -2047 && deltaOfDelta <= 2048 {
		// '1110' + 12 bits
		e.w.WriteBits(0b1110, 4)
		e.w.WriteBits(uint64(deltaOfDelta)&0xFFF, 12)
	} else {
		// '1111' + 32 bits
		e.w.WriteBits(0b1111, 4)
		e.w.WriteBits(uint64(deltaOfDelta)&0xFFFFFFFF, 32)
	}

	e.count++
	return nil
}

// Finish finalizes the encoding and returns the compressed bytes.
// Returns a copy to avoid issues with buffer reuse after Reset().
func (e *TimestampEncoder) Finish() []byte {
	e.finalized = true
	data := e.w.Bytes()
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

// Count returns the number of timestamps encoded.
func (e *TimestampEncoder) Count() int {
	return e.count
}

// Reset resets the encoder for reuse.
func (e *TimestampEncoder) Reset() {
	e.w.Reset()
	e.tPrev = 0
	e.tDelta = 0
	e.count = 0
	e.finalized = false
}

// TimestampDecoder decodes timestamps compressed with TimestampEncoder.
type TimestampDecoder struct {
	r      *BitReader
	tPrev  int64 // Previous timestamp
	tDelta int64 // Previous delta
	count  int   // Number of timestamps decoded
	err    error // Last error encountered
}

// NewTimestampDecoder creates a new timestamp decoder from compressed data.
func NewTimestampDecoder(data []byte) *TimestampDecoder {
	return &TimestampDecoder{
		r: NewBitReader(data),
	}
}

// Decode reads the next timestamp.
func (d *TimestampDecoder) Decode() (int64, error) {
	if d.err != nil {
		return 0, d.err
	}

	if d.count == 0 {
		// First timestamp: read as-is
		val, err := d.r.ReadUint64()
		if err != nil {
			d.err = err
			return 0, err
		}
		d.tPrev = int64(val)
		d.count++
		return d.tPrev, nil
	}

	if d.count == 1 {
		// Second timestamp: read delta
		val, err := d.r.ReadUint64()
		if err != nil {
			d.err = err
			return 0, err
		}
		d.tDelta = int64(val)
		d.tPrev += d.tDelta
		d.count++
		return d.tPrev, nil
	}

	// Read delta-of-delta using variable-length decoding
	var deltaOfDelta int64

	bit, err := d.r.ReadBit()
	if err != nil {
		d.err = err
		return 0, err
	}

	if bit == 0 {
		// Delta-of-delta is 0
		deltaOfDelta = 0
	} else {
		// Read next bit
		bit2, err := d.r.ReadBit()
		if err != nil {
			d.err = err
			return 0, err
		}

		if bit2 == 0 {
			// '10' - read 7 bits
			val, err := d.r.ReadBits(7)
			if err != nil {
				d.err = err
				return 0, err
			}
			deltaOfDelta = signExtend(val, 7)
		} else {
			// Read next bit
			bit3, err := d.r.ReadBit()
			if err != nil {
				d.err = err
				return 0, err
			}

			if bit3 == 0 {
				// '110' - read 9 bits
				val, err := d.r.ReadBits(9)
				if err != nil {
					d.err = err
					return 0, err
				}
				deltaOfDelta = signExtend(val, 9)
			} else {
				// Read next bit
				bit4, err := d.r.ReadBit()
				if err != nil {
					d.err = err
					return 0, err
				}

				if bit4 == 0 {
					// '1110' - read 12 bits
					val, err := d.r.ReadBits(12)
					if err != nil {
						d.err = err
						return 0, err
					}
					deltaOfDelta = signExtend(val, 12)
				} else {
					// '1111' - read 32 bits
					val, err := d.r.ReadBits(32)
					if err != nil {
						d.err = err
						return 0, err
					}
					deltaOfDelta = signExtend(val, 32)
				}
			}
		}
	}

	// Compute timestamp
	d.tDelta += deltaOfDelta
	d.tPrev += d.tDelta
	d.count++

	return d.tPrev, nil
}

// Count returns the number of timestamps decoded so far.
func (d *TimestampDecoder) Count() int {
	return d.count
}

// Err returns the last error encountered during decoding.
func (d *TimestampDecoder) Err() error {
	return d.err
}

// Reset resets the decoder to decode a new buffer.
func (d *TimestampDecoder) Reset(data []byte) {
	d.r.Reset(data)
	d.tPrev = 0
	d.tDelta = 0
	d.count = 0
	d.err = nil
}

// signExtend performs sign extension on a value with the given bit width.
// For example, signExtend(0b1111111, 7) returns -1.
func signExtend(val uint64, bits uint8) int64 {
	// Check if the sign bit is set
	signBit := uint64(1) << (bits - 1)
	if val&signBit != 0 {
		// Sign bit is set, extend with 1s
		mask := uint64(math.MaxUint64) << bits
		return int64(val | mask)
	}
	// Sign bit is not set
	return int64(val)
}
