package compression

import (
	"bytes"
	"fmt"
	"io"
	"math"
)

// ValueEncoder implements XOR compression for float64 values as described
// in Facebook's Gorilla paper. This achieves excellent compression for slowly
// changing numerical values.
//
// Algorithm:
// - Store first value as-is (64 bits)
// - For subsequent values:
//   - XOR with previous value
//   - If XOR is 0: write 1 bit (0)
//   - Otherwise: write 1 bit (1) followed by:
//     - If leading/trailing zeros match previous: write 1 bit (0) + significant bits
//     - Otherwise: write 1 bit (1) + 5 bits (leading zeros) + 6 bits (block size) + significant bits
type ValueEncoder struct {
	bw           *BitWriter
	prevValue    uint64 // Previous value as uint64 (bit representation)
	prevLeading  uint8  // Leading zeros in previous XOR
	prevTrailing uint8  // Trailing zeros in previous XOR
	count        int    // Number of values encoded
}

// NewValueEncoder creates a new value encoder
func NewValueEncoder() *ValueEncoder {
	buf := &bytes.Buffer{}
	return &ValueEncoder{
		bw: NewBitWriter(buf),
	}
}

// Encode encodes a float64 value using XOR compression
func (e *ValueEncoder) Encode(v float64) error {
	// Convert float64 to uint64 bit representation
	vBits := math.Float64bits(v)

	if e.count == 0 {
		// First value: store as-is
		e.prevValue = vBits
		e.count++
		return e.bw.WriteBits(vBits, 64)
	}

	// XOR with previous value
	xor := vBits ^ e.prevValue
	e.prevValue = vBits
	e.count++

	if xor == 0 {
		// Value hasn't changed: write 1 bit (0)
		return e.bw.WriteBit(0)
	}

	// Value changed: write 1 bit (1)
	if err := e.bw.WriteBit(1); err != nil {
		return err
	}

	// Count leading and trailing zeros
	leading := uint8(countLeadingZeros(xor))
	trailing := uint8(countTrailingZeros(xor))

	// Check if we can use the same leading/trailing as previous
	if e.count > 1 && leading >= e.prevLeading && trailing >= e.prevTrailing {
		// Use previous block: write 1 bit (0) + significant bits
		if err := e.bw.WriteBit(0); err != nil {
			return err
		}

		// Write significant bits (between leading and trailing zeros)
		blockSize := 64 - e.prevLeading - e.prevTrailing
		block := (xor >> e.prevTrailing) & ((1 << blockSize) - 1)

		return e.bw.WriteBits(block, blockSize)
	}

	// New block: write 1 bit (1) + 5 bits (leading) + 6 bits (block size) + significant bits
	if err := e.bw.WriteBit(1); err != nil {
		return err
	}

	// Write leading zeros count (5 bits, max 31)
	if err := e.bw.WriteBits(uint64(leading), 5); err != nil {
		return err
	}

	// Calculate and write block size
	blockSize := 64 - leading - trailing
	if blockSize > 63 {
		blockSize = 63 // Maximum block size
	}

	// Write block size (6 bits, representing 1-64)
	if err := e.bw.WriteBits(uint64(blockSize), 6); err != nil {
		return err
	}

	// Write significant bits
	block := (xor >> trailing) & ((1 << blockSize) - 1)
	if err := e.bw.WriteBits(block, blockSize); err != nil {
		return err
	}

	// Update state
	e.prevLeading = leading
	e.prevTrailing = trailing

	return nil
}

// Finish finalizes the encoding and returns the compressed bytes
func (e *ValueEncoder) Finish() ([]byte, error) {
	if err := e.bw.Flush(); err != nil {
		return nil, err
	}

	// Extract bytes from buffer
	if buf, ok := e.bw.w.(*bytes.Buffer); ok {
		return buf.Bytes(), nil
	}

	return nil, fmt.Errorf("unexpected writer type")
}

// Count returns the number of values encoded
func (e *ValueEncoder) Count() int {
	return e.count
}

// BitsWritten returns the total bits written
func (e *ValueEncoder) BitsWritten() uint64 {
	return e.bw.BitsWritten()
}

// ValueDecoder implements XOR decompression for float64 values
type ValueDecoder struct {
	br           *BitReader
	prevValue    uint64 // Previous value as uint64
	prevLeading  uint8  // Leading zeros in previous XOR
	prevTrailing uint8  // Trailing zeros in previous XOR
	count        int    // Number of values decoded
}

// NewValueDecoder creates a new value decoder
func NewValueDecoder(data []byte) *ValueDecoder {
	return &ValueDecoder{
		br: NewBitReader(data),
	}
}

// Decode decodes the next float64 value
func (d *ValueDecoder) Decode() (float64, error) {
	if d.count == 0 {
		// First value: read 64 bits
		val, err := d.br.ReadBits(64)
		if err != nil {
			return 0, err
		}
		d.prevValue = val
		d.count++
		return math.Float64frombits(val), nil
	}

	// Read control bit
	bit, err := d.br.ReadBit()
	if err != nil {
		return 0, err
	}

	var xor uint64

	if bit == 0 {
		// Value hasn't changed: xor = 0
		xor = 0
	} else {
		// Value changed: read another control bit
		bit2, err := d.br.ReadBit()
		if err != nil {
			return 0, err
		}

		if bit2 == 0 {
			// Use previous block size
			blockSize := 64 - d.prevLeading - d.prevTrailing
			block, err := d.br.ReadBits(blockSize)
			if err != nil {
				return 0, err
			}

			xor = block << d.prevTrailing
		} else {
			// New block: read leading zeros (5 bits) and block size (6 bits)
			leading, err := d.br.ReadBits(5)
			if err != nil {
				return 0, err
			}

			blockSize, err := d.br.ReadBits(6)
			if err != nil {
				return 0, err
			}

			block, err := d.br.ReadBits(uint8(blockSize))
			if err != nil {
				return 0, err
			}

			d.prevLeading = uint8(leading)
			d.prevTrailing = 64 - d.prevLeading - uint8(blockSize)

			xor = block << d.prevTrailing
		}
	}

	// Reconstruct value
	d.prevValue = d.prevValue ^ xor
	d.count++

	return math.Float64frombits(d.prevValue), nil
}

// DecodeAll decodes all values and returns them as a slice
func (d *ValueDecoder) DecodeAll(count int) ([]float64, error) {
	values := make([]float64, 0, count)

	for i := 0; i < count; i++ {
		v, err := d.Decode()
		if err != nil {
			if err == io.EOF && i == count {
				break
			}
			return nil, fmt.Errorf("failed to decode value %d: %w", i, err)
		}
		values = append(values, v)
	}

	return values, nil
}

// Count returns the number of values decoded
func (d *ValueDecoder) Count() int {
	return d.count
}

// countLeadingZeros counts the number of leading zero bits in a uint64
func countLeadingZeros(x uint64) int {
	if x == 0 {
		return 64
	}

	n := 0
	if x&0xFFFFFFFF00000000 == 0 {
		n += 32
		x <<= 32
	}
	if x&0xFFFF000000000000 == 0 {
		n += 16
		x <<= 16
	}
	if x&0xFF00000000000000 == 0 {
		n += 8
		x <<= 8
	}
	if x&0xF000000000000000 == 0 {
		n += 4
		x <<= 4
	}
	if x&0xC000000000000000 == 0 {
		n += 2
		x <<= 2
	}
	if x&0x8000000000000000 == 0 {
		n += 1
	}

	return n
}

// countTrailingZeros counts the number of trailing zero bits in a uint64
func countTrailingZeros(x uint64) int {
	if x == 0 {
		return 64
	}

	n := 0
	if x&0x00000000FFFFFFFF == 0 {
		n += 32
		x >>= 32
	}
	if x&0x000000000000FFFF == 0 {
		n += 16
		x >>= 16
	}
	if x&0x00000000000000FF == 0 {
		n += 8
		x >>= 8
	}
	if x&0x000000000000000F == 0 {
		n += 4
		x >>= 4
	}
	if x&0x0000000000000003 == 0 {
		n += 2
		x >>= 2
	}
	if x&0x0000000000000001 == 0 {
		n += 1
	}

	return n
}
