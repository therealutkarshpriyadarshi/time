package compression

import (
	"fmt"
	"io"
)

// BitWriter provides bit-level writing capabilities for compression algorithms.
// It buffers bits and writes full bytes to the underlying writer.
type BitWriter struct {
	w     io.Writer
	buf   byte   // Current byte being written
	count uint8  // Number of bits written to buf (0-7)
	total uint64 // Total bits written
}

// NewBitWriter creates a new BitWriter
func NewBitWriter(w io.Writer) *BitWriter {
	return &BitWriter{w: w}
}

// WriteBit writes a single bit (0 or 1)
func (bw *BitWriter) WriteBit(bit uint8) error {
	if bit&1 == 1 {
		bw.buf |= 1 << (7 - bw.count)
	}

	bw.count++
	bw.total++

	if bw.count == 8 {
		if err := bw.flush(); err != nil {
			return err
		}
	}

	return nil
}

// WriteBits writes n bits from value (least significant bits)
func (bw *BitWriter) WriteBits(value uint64, n uint8) error {
	if n > 64 {
		return fmt.Errorf("cannot write more than 64 bits at once")
	}

	for i := n; i > 0; i-- {
		bit := uint8((value >> (i - 1)) & 1)
		if err := bw.WriteBit(bit); err != nil {
			return err
		}
	}

	return nil
}

// WriteByte writes a full byte (8 bits)
func (bw *BitWriter) WriteByte(b byte) error {
	return bw.WriteBits(uint64(b), 8)
}

// Flush writes any remaining bits (padding with zeros if necessary)
func (bw *BitWriter) Flush() error {
	if bw.count > 0 {
		return bw.flush()
	}
	return nil
}

// flush writes the current buffer to the writer
func (bw *BitWriter) flush() error {
	_, err := bw.w.Write([]byte{bw.buf})
	if err != nil {
		return err
	}

	bw.buf = 0
	bw.count = 0

	return nil
}

// BitsWritten returns the total number of bits written
func (bw *BitWriter) BitsWritten() uint64 {
	return bw.total
}

// BitReader provides bit-level reading capabilities for decompression algorithms.
type BitReader struct {
	data  []byte
	pos   int    // Current byte position
	count uint8  // Number of bits read from current byte (0-7)
	total uint64 // Total bits read
}

// NewBitReader creates a new BitReader from a byte slice
func NewBitReader(data []byte) *BitReader {
	return &BitReader{data: data}
}

// ReadBit reads a single bit (0 or 1)
func (br *BitReader) ReadBit() (uint8, error) {
	if br.pos >= len(br.data) {
		return 0, io.EOF
	}

	bit := (br.data[br.pos] >> (7 - br.count)) & 1
	br.count++
	br.total++

	if br.count == 8 {
		br.pos++
		br.count = 0
	}

	return bit, nil
}

// ReadBits reads n bits into a uint64
func (br *BitReader) ReadBits(n uint8) (uint64, error) {
	if n > 64 {
		return 0, fmt.Errorf("cannot read more than 64 bits at once")
	}

	var value uint64
	for i := uint8(0); i < n; i++ {
		bit, err := br.ReadBit()
		if err != nil {
			return 0, err
		}
		value = (value << 1) | uint64(bit)
	}

	return value, nil
}

// ReadByte reads 8 bits as a byte
func (br *BitReader) ReadByte() (byte, error) {
	val, err := br.ReadBits(8)
	return byte(val), err
}

// BitsRead returns the total number of bits read
func (br *BitReader) BitsRead() uint64 {
	return br.total
}

// Reset resets the reader to the beginning
func (br *BitReader) Reset(data []byte) {
	br.data = data
	br.pos = 0
	br.count = 0
	br.total = 0
}
