package compression

import (
	"encoding/binary"
	"fmt"
	"io"
)

// BitWriter provides bit-level writing for compression algorithms.
// Used by Gorilla timestamp and value encoders.
type BitWriter struct {
	buf      []byte // Buffer for writing
	bytePos  int    // Current byte position
	bitPos   uint8  // Current bit position within the byte (0-7)
	capacity int    // Current capacity
}

// NewBitWriter creates a new bit-level writer with initial capacity.
func NewBitWriter(capacity int) *BitWriter {
	if capacity <= 0 {
		capacity = 128 // Default 128 bytes
	}
	return &BitWriter{
		buf:      make([]byte, 0, capacity),
		capacity: capacity,
	}
}

// WriteBit writes a single bit (0 or 1).
func (w *BitWriter) WriteBit(bit uint64) {
	w.WriteBits(bit, 1)
}

// WriteBits writes the least significant n bits of value.
// Maximum n is 64 bits.
func (w *BitWriter) WriteBits(value uint64, n uint8) {
	if n == 0 || n > 64 {
		return
	}

	// Ensure we have enough capacity
	w.ensureCapacity(int(n))

	// Write bits from most significant to least significant
	for i := n; i > 0; i-- {
		// Get the bit at position (i-1) from the right
		bit := (value >> (i - 1)) & 1

		// Ensure we have a byte to write to
		if w.bytePos >= len(w.buf) {
			w.buf = append(w.buf, 0)
		}

		// Set the bit at the current position
		if bit == 1 {
			w.buf[w.bytePos] |= 1 << (7 - w.bitPos)
		}

		// Move to next bit position
		w.bitPos++
		if w.bitPos >= 8 {
			w.bitPos = 0
			w.bytePos++
		}
	}
}

// WriteUint64 writes a 64-bit unsigned integer.
func (w *BitWriter) WriteUint64(value uint64) {
	w.WriteBits(value, 64)
}

// WriteUint32 writes a 32-bit unsigned integer.
func (w *BitWriter) WriteUint32(value uint32) {
	w.WriteBits(uint64(value), 32)
}

// WriteUint16 writes a 16-bit unsigned integer.
func (w *BitWriter) WriteUint16(value uint16) {
	w.WriteBits(uint64(value), 16)
}

// WriteUint8 writes an 8-bit unsigned integer.
func (w *BitWriter) WriteUint8(value uint8) {
	w.WriteBits(uint64(value), 8)
}

// ensureCapacity ensures the buffer has enough capacity for n more bits.
func (w *BitWriter) ensureCapacity(n int) {
	requiredBytes := (n + int(w.bitPos) + 7) / 8
	if w.bytePos+requiredBytes > cap(w.buf) {
		// Double capacity until we have enough
		newCap := cap(w.buf) * 2
		for w.bytePos+requiredBytes > newCap {
			newCap *= 2
		}
		newBuf := make([]byte, len(w.buf), newCap)
		copy(newBuf, w.buf)
		w.buf = newBuf
	}
}

// Bytes returns the written bytes. The buffer may not be byte-aligned.
func (w *BitWriter) Bytes() []byte {
	// If we're in the middle of a byte, include it
	if w.bitPos > 0 && w.bytePos < len(w.buf) {
		return w.buf[:w.bytePos+1]
	}
	return w.buf[:w.bytePos]
}

// BitLength returns the total number of bits written.
func (w *BitWriter) BitLength() int {
	return w.bytePos*8 + int(w.bitPos)
}

// Reset resets the writer to start writing from the beginning.
func (w *BitWriter) Reset() {
	w.buf = w.buf[:0]
	w.bytePos = 0
	w.bitPos = 0
}

// BitReader provides bit-level reading for decompression algorithms.
// Used by Gorilla timestamp and value decoders.
type BitReader struct {
	buf     []byte // Buffer for reading
	bytePos int    // Current byte position
	bitPos  uint8  // Current bit position within the byte (0-7)
	err     error  // Last error encountered
}

// NewBitReader creates a new bit-level reader from the given buffer.
func NewBitReader(buf []byte) *BitReader {
	return &BitReader{
		buf: buf,
	}
}

// ReadBit reads a single bit (0 or 1).
func (r *BitReader) ReadBit() (uint64, error) {
	return r.ReadBits(1)
}

// ReadBits reads n bits as a uint64.
// Maximum n is 64 bits.
func (r *BitReader) ReadBits(n uint8) (uint64, error) {
	if r.err != nil {
		return 0, r.err
	}

	if n == 0 || n > 64 {
		r.err = fmt.Errorf("invalid bit count: %d", n)
		return 0, r.err
	}

	var value uint64

	for i := uint8(0); i < n; i++ {
		// Check if we've reached the end of the buffer
		if r.bytePos >= len(r.buf) {
			r.err = io.EOF
			return 0, r.err
		}

		// Read the bit at the current position
		bit := (r.buf[r.bytePos] >> (7 - r.bitPos)) & 1
		value = (value << 1) | uint64(bit)

		// Move to next bit position
		r.bitPos++
		if r.bitPos >= 8 {
			r.bitPos = 0
			r.bytePos++
		}
	}

	return value, nil
}

// ReadUint64 reads a 64-bit unsigned integer.
func (r *BitReader) ReadUint64() (uint64, error) {
	return r.ReadBits(64)
}

// ReadUint32 reads a 32-bit unsigned integer.
func (r *BitReader) ReadUint32() (uint32, error) {
	val, err := r.ReadBits(32)
	return uint32(val), err
}

// ReadUint16 reads a 16-bit unsigned integer.
func (r *BitReader) ReadUint16() (uint16, error) {
	val, err := r.ReadBits(16)
	return uint16(val), err
}

// ReadUint8 reads an 8-bit unsigned integer.
func (r *BitReader) ReadUint8() (uint8, error) {
	val, err := r.ReadBits(8)
	return uint8(val), err
}

// Err returns the last error encountered during reading.
func (r *BitReader) Err() error {
	return r.err
}

// BytesRead returns the number of bytes read so far.
func (r *BitReader) BytesRead() int {
	if r.bitPos > 0 {
		return r.bytePos + 1
	}
	return r.bytePos
}

// BitPosition returns the current bit position in the stream.
func (r *BitReader) BitPosition() int {
	return r.bytePos*8 + int(r.bitPos)
}

// Reset resets the reader to start reading from the beginning with a new buffer.
func (r *BitReader) Reset(buf []byte) {
	r.buf = buf
	r.bytePos = 0
	r.bitPos = 0
	r.err = nil
}

// WriteTo writes the remaining bytes to the writer.
// This is useful for implementing io.WriterTo interface.
func (w *BitWriter) WriteTo(wr io.Writer) (int64, error) {
	n, err := wr.Write(w.Bytes())
	return int64(n), err
}

// ReadFrom reads from the reader until EOF and stores in the BitReader.
// This is useful for implementing io.ReaderFrom interface.
func (r *BitReader) ReadFrom(rd io.Reader) (int64, error) {
	// Read all data from reader
	data := make([]byte, 0, 1024)
	buf := make([]byte, 1024)
	var total int64

	for {
		n, err := rd.Read(buf)
		if n > 0 {
			data = append(data, buf[:n]...)
			total += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return total, err
		}
	}

	r.Reset(data)
	return total, nil
}

// Helper function to convert byte slice to uint64 (big-endian)
func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		var tmp [8]byte
		copy(tmp[8-len(b):], b)
		return binary.BigEndian.Uint64(tmp[:])
	}
	return binary.BigEndian.Uint64(b)
}

// Helper function to convert uint64 to byte slice (big-endian)
func uint64ToBytes(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}
