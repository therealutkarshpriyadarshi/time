package compression

import (
	"bytes"
	"io"
	"testing"
)

func TestBitWriter_WriteBit(t *testing.T) {
	w := NewBitWriter(1)

	// Write pattern: 10101010
	bits := []uint64{1, 0, 1, 0, 1, 0, 1, 0}
	for _, bit := range bits {
		w.WriteBit(bit)
	}

	result := w.Bytes()
	if len(result) != 1 {
		t.Fatalf("Expected 1 byte, got %d", len(result))
	}

	expected := byte(0b10101010)
	if result[0] != expected {
		t.Errorf("Expected %08b, got %08b", expected, result[0])
	}
}

func TestBitWriter_WriteBits(t *testing.T) {
	tests := []struct {
		name     string
		writes   []struct{ value uint64; bits uint8 }
		expected []byte
	}{
		{
			name: "Single byte",
			writes: []struct{ value uint64; bits uint8 }{
				{0b11110000, 8},
			},
			expected: []byte{0b11110000},
		},
		{
			name: "Multiple small writes",
			writes: []struct{ value uint64; bits uint8 }{
				{0b111, 3},
				{0b000, 3},
				{0b11, 2},
			},
			expected: []byte{0b11100011},
		},
		{
			name: "Cross byte boundary",
			writes: []struct{ value uint64; bits uint8 }{
				{0b11111111, 8},
				{0b10101010, 8},
			},
			expected: []byte{0b11111111, 0b10101010},
		},
		{
			name: "64-bit write",
			writes: []struct{ value uint64; bits uint8 }{
				{0x0123456789ABCDEF, 64},
			},
			expected: []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBitWriter(8)
			for _, write := range tt.writes {
				w.WriteBits(write.value, write.bits)
			}

			result := w.Bytes()
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBitWriter_WriteUintTypes(t *testing.T) {
	w := NewBitWriter(16)

	w.WriteUint8(0xAB)
	w.WriteUint16(0xCDEF)
	w.WriteUint32(0x12345678)
	w.WriteUint64(0x0123456789ABCDEF)

	expected := []byte{
		0xAB,                   // uint8
		0xCD, 0xEF,             // uint16
		0x12, 0x34, 0x56, 0x78, // uint32
		0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, // uint64
	}

	result := w.Bytes()
	if !bytes.Equal(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestBitWriter_BitLength(t *testing.T) {
	w := NewBitWriter(8)

	if w.BitLength() != 0 {
		t.Errorf("Expected 0 bits, got %d", w.BitLength())
	}

	w.WriteBits(0b111, 3)
	if w.BitLength() != 3 {
		t.Errorf("Expected 3 bits, got %d", w.BitLength())
	}

	w.WriteBits(0b11111111, 8)
	if w.BitLength() != 11 {
		t.Errorf("Expected 11 bits, got %d", w.BitLength())
	}

	w.WriteBits(0b101, 3)
	if w.BitLength() != 14 {
		t.Errorf("Expected 14 bits, got %d", w.BitLength())
	}
}

func TestBitWriter_Reset(t *testing.T) {
	w := NewBitWriter(8)
	w.WriteBits(0xFF, 8)

	w.Reset()

	if w.BitLength() != 0 {
		t.Errorf("Expected 0 bits after reset, got %d", w.BitLength())
	}

	if len(w.Bytes()) != 0 {
		t.Errorf("Expected empty bytes after reset, got %d bytes", len(w.Bytes()))
	}
}

func TestBitReader_ReadBit(t *testing.T) {
	// Test reading pattern: 10101010
	buf := []byte{0b10101010}
	r := NewBitReader(buf)

	expected := []uint64{1, 0, 1, 0, 1, 0, 1, 0}
	for i, exp := range expected {
		bit, err := r.ReadBit()
		if err != nil {
			t.Fatalf("Error reading bit %d: %v", i, err)
		}
		if bit != exp {
			t.Errorf("Bit %d: expected %d, got %d", i, exp, bit)
		}
	}
}

func TestBitReader_ReadBits(t *testing.T) {
	tests := []struct {
		name     string
		buf      []byte
		reads    []struct{ bits uint8; expected uint64 }
	}{
		{
			name: "Single byte",
			buf:  []byte{0b11110000},
			reads: []struct{ bits uint8; expected uint64 }{
				{8, 0b11110000},
			},
		},
		{
			name: "Multiple small reads",
			buf:  []byte{0b11100011},
			reads: []struct{ bits uint8; expected uint64 }{
				{3, 0b111},
				{3, 0b000},
				{2, 0b11},
			},
		},
		{
			name: "Cross byte boundary",
			buf:  []byte{0b11111111, 0b10101010},
			reads: []struct{ bits uint8; expected uint64 }{
				{8, 0b11111111},
				{8, 0b10101010},
			},
		},
		{
			name: "64-bit read",
			buf:  []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF},
			reads: []struct{ bits uint8; expected uint64 }{
				{64, 0x0123456789ABCDEF},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBitReader(tt.buf)
			for i, read := range tt.reads {
				value, err := r.ReadBits(read.bits)
				if err != nil {
					t.Fatalf("Read %d failed: %v", i, err)
				}
				if value != read.expected {
					t.Errorf("Read %d: expected 0x%X, got 0x%X", i, read.expected, value)
				}
			}
		})
	}
}

func TestBitReader_ReadUintTypes(t *testing.T) {
	buf := []byte{
		0xAB,                   // uint8
		0xCD, 0xEF,             // uint16
		0x12, 0x34, 0x56, 0x78, // uint32
		0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, // uint64
	}

	r := NewBitReader(buf)

	val8, err := r.ReadUint8()
	if err != nil || val8 != 0xAB {
		t.Errorf("ReadUint8: expected 0xAB, got 0x%X (err: %v)", val8, err)
	}

	val16, err := r.ReadUint16()
	if err != nil || val16 != 0xCDEF {
		t.Errorf("ReadUint16: expected 0xCDEF, got 0x%X (err: %v)", val16, err)
	}

	val32, err := r.ReadUint32()
	if err != nil || val32 != 0x12345678 {
		t.Errorf("ReadUint32: expected 0x12345678, got 0x%X (err: %v)", val32, err)
	}

	val64, err := r.ReadUint64()
	if err != nil || val64 != 0x0123456789ABCDEF {
		t.Errorf("ReadUint64: expected 0x0123456789ABCDEF, got 0x%X (err: %v)", val64, err)
	}
}

func TestBitReader_EOF(t *testing.T) {
	buf := []byte{0xFF}
	r := NewBitReader(buf)

	// Read the byte
	_, err := r.ReadBits(8)
	if err != nil {
		t.Fatalf("First read failed: %v", err)
	}

	// Try to read beyond buffer
	_, err = r.ReadBit()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestBitReader_Reset(t *testing.T) {
	buf1 := []byte{0xAB}
	buf2 := []byte{0xCD}

	r := NewBitReader(buf1)

	val, _ := r.ReadUint8()
	if val != 0xAB {
		t.Errorf("Expected 0xAB, got 0x%X", val)
	}

	r.Reset(buf2)

	val, _ = r.ReadUint8()
	if val != 0xCD {
		t.Errorf("Expected 0xCD after reset, got 0x%X", val)
	}
}

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		writes []struct{ value uint64; bits uint8 }
	}{
		{
			name: "Various bit lengths",
			writes: []struct{ value uint64; bits uint8 }{
				{0b1, 1},
				{0b11, 2},
				{0b111, 3},
				{0b1111, 4},
				{0b11111, 5},
				{0xFF, 8},
				{0xFFFF, 16},
				{0xFFFFFFFF, 32},
				{0xFFFFFFFFFFFFFFFF, 64},
			},
		},
		{
			name: "Mixed values",
			writes: []struct{ value uint64; bits uint8 }{
				{0x12, 8},
				{0x3456, 16},
				{0x789ABCDE, 32},
				{0xF0, 8},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewBitWriter(64)

			// Write all values
			for _, write := range tt.writes {
				w.WriteBits(write.value, write.bits)
			}

			// Read them back
			r := NewBitReader(w.Bytes())
			for i, write := range tt.writes {
				value, err := r.ReadBits(write.bits)
				if err != nil {
					t.Fatalf("Read %d failed: %v", i, err)
				}

				// Mask the expected value to the correct bit width
				mask := uint64((1 << write.bits) - 1)
				expected := write.value & mask

				if value != expected {
					t.Errorf("Read %d: expected 0x%X, got 0x%X", i, expected, value)
				}
			}
		})
	}
}

func TestBitWriter_WriteTo(t *testing.T) {
	w := NewBitWriter(8)
	w.WriteBits(0xABCD, 16)

	var buf bytes.Buffer
	n, err := w.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	if n != 2 {
		t.Errorf("Expected 2 bytes written, got %d", n)
	}

	expected := []byte{0xAB, 0xCD}
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Expected %v, got %v", expected, buf.Bytes())
	}
}

func TestBitReader_ReadFrom(t *testing.T) {
	data := []byte{0xAB, 0xCD, 0xEF}
	buf := bytes.NewReader(data)

	r := NewBitReader(nil)
	n, err := r.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if n != 3 {
		t.Errorf("Expected 3 bytes read, got %d", n)
	}

	// Verify we can read the data
	val, err := r.ReadBits(24)
	if err != nil {
		t.Fatalf("ReadBits failed: %v", err)
	}

	expected := uint64(0xABCDEF)
	if val != expected {
		t.Errorf("Expected 0x%X, got 0x%X", expected, val)
	}
}

func TestBitWriter_NonByteAligned(t *testing.T) {
	w := NewBitWriter(8)

	// Write 3 bits (not byte-aligned)
	w.WriteBits(0b111, 3)

	result := w.Bytes()
	if len(result) != 1 {
		t.Fatalf("Expected 1 byte, got %d", len(result))
	}

	// The 3 bits should be in the high bits: 11100000
	expected := byte(0b11100000)
	if result[0] != expected {
		t.Errorf("Expected %08b, got %08b", expected, result[0])
	}

	if w.BitLength() != 3 {
		t.Errorf("Expected 3 bits, got %d", w.BitLength())
	}
}

func TestBitReader_BitPosition(t *testing.T) {
	buf := []byte{0xFF, 0xFF}
	r := NewBitReader(buf)

	if r.BitPosition() != 0 {
		t.Errorf("Expected position 0, got %d", r.BitPosition())
	}

	r.ReadBits(3)
	if r.BitPosition() != 3 {
		t.Errorf("Expected position 3, got %d", r.BitPosition())
	}

	r.ReadBits(8)
	if r.BitPosition() != 11 {
		t.Errorf("Expected position 11, got %d", r.BitPosition())
	}
}

// Benchmark tests
func BenchmarkBitWriter_WriteBits(b *testing.B) {
	w := NewBitWriter(1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.WriteBits(0x123456, 24)
		if i%1000 == 0 {
			w.Reset()
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
}

func BenchmarkBitReader_ReadBits(b *testing.B) {
	// Create a large buffer
	w := NewBitWriter(10000)
	for i := 0; i < 1000; i++ {
		w.WriteBits(0x123456, 24)
	}
	buf := w.Bytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := NewBitReader(buf)
		for j := 0; j < 1000; j++ {
			r.ReadBits(24)
		}
	}

	b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "reads/sec")
}
