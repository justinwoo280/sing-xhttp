package xhttp

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchAccumulatorBasic(t *testing.T) {
	acc := newBatchAccumulator(1024)
	defer acc.Close()

	data := []byte("hello world")
	n, err := acc.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("wrote %d, want %d", n, len(data))
	}

	chunk, err := acc.Drain()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(chunk, data) {
		t.Fatalf("drained %q, want %q", chunk, data)
	}
}

func TestBatchAccumulatorMultipleSmallWrites(t *testing.T) {
	// Multiple small writes should be merged into a single Drain result.
	acc := newBatchAccumulator(1024)

	// Write 10 small chunks rapidly.
	for i := 0; i < 10; i++ {
		if _, err := acc.Write([]byte("ab")); err != nil {
			t.Fatal(err)
		}
	}

	// Single Drain should return all 20 bytes merged.
	chunk, err := acc.Drain()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk) != 20 {
		t.Fatalf("drained %d bytes, want 20", len(chunk))
	}
	expected := bytes.Repeat([]byte("ab"), 10)
	if !bytes.Equal(chunk, expected) {
		t.Fatalf("data mismatch")
	}
	acc.Close()
}

func TestBatchAccumulatorMaxBatch(t *testing.T) {
	// Drain should return at most maxBatch bytes.
	acc := newBatchAccumulator(100)

	// Write 300 bytes.
	data := bytes.Repeat([]byte("x"), 300)
	if _, err := acc.Write(data); err != nil {
		t.Fatal(err)
	}

	// First drain: 100 bytes.
	chunk1, err := acc.Drain()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk1) != 100 {
		t.Fatalf("drain 1: got %d bytes, want 100", len(chunk1))
	}

	// Second drain: 100 bytes.
	chunk2, err := acc.Drain()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk2) != 100 {
		t.Fatalf("drain 2: got %d bytes, want 100", len(chunk2))
	}

	// Third drain: 100 bytes.
	chunk3, err := acc.Drain()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk3) != 100 {
		t.Fatalf("drain 3: got %d bytes, want 100", len(chunk3))
	}

	acc.Close()
}

func TestBatchAccumulatorBackpressure(t *testing.T) {
	// Write should block when buffer >= maxBatch.
	acc := newBatchAccumulator(100)

	// Fill the buffer to maxBatch.
	if _, err := acc.Write(bytes.Repeat([]byte("x"), 100)); err != nil {
		t.Fatal(err)
	}

	// Next write should block.
	blocked := make(chan struct{})
	go func() {
		acc.Write([]byte("y"))
		close(blocked)
	}()

	// Verify it's blocked (give it a moment).
	select {
	case <-blocked:
		t.Fatal("Write should have blocked")
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	// Drain to unblock.
	chunk, err := acc.Drain()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk) != 100 {
		t.Fatalf("drained %d, want 100", len(chunk))
	}

	// Write should now complete.
	select {
	case <-blocked:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Write should have unblocked after Drain")
	}

	acc.Close()
}

func TestBatchAccumulatorCloseFlush(t *testing.T) {
	// After Close, Drain should return remaining data then EOF.
	acc := newBatchAccumulator(1024)

	data := []byte("remaining data")
	if _, err := acc.Write(data); err != nil {
		t.Fatal(err)
	}
	acc.Close()

	// Should get the remaining data.
	chunk, err := acc.Drain()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(chunk, data) {
		t.Fatalf("got %q, want %q", chunk, data)
	}

	// Next drain should return EOF.
	_, err = acc.Drain()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestBatchAccumulatorCloseEmpty(t *testing.T) {
	// Close with empty buffer, Drain should return EOF.
	acc := newBatchAccumulator(1024)

	done := make(chan error, 1)
	go func() {
		_, err := acc.Drain()
		done <- err
	}()

	// Give Drain time to block.
	time.Sleep(20 * time.Millisecond)
	acc.Close()

	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Drain should have returned after Close")
	}
}

func TestBatchAccumulatorCloseWithError(t *testing.T) {
	acc := newBatchAccumulator(1024)

	testErr := io.ErrUnexpectedEOF
	acc.CloseWithError(testErr)

	// Write should return the error.
	_, err := acc.Write([]byte("x"))
	if err != testErr {
		t.Fatalf("expected %v, got %v", testErr, err)
	}
}

func TestBatchAccumulatorCloseIdempotent(t *testing.T) {
	acc := newBatchAccumulator(1024)

	// CloseWithError, then Close should preserve original error.
	testErr := io.ErrShortBuffer
	acc.CloseWithError(testErr)
	acc.Close() // no-op

	_, err := acc.Write([]byte("x"))
	if err != testErr {
		t.Fatalf("expected %v, got %v", testErr, err)
	}
}

func TestBatchAccumulatorWriteAfterClose(t *testing.T) {
	acc := newBatchAccumulator(1024)
	acc.Close()

	_, err := acc.Write([]byte("x"))
	if err != io.ErrClosedPipe {
		t.Fatalf("expected io.ErrClosedPipe, got %v", err)
	}
}

func TestBatchAccumulatorConcurrentWriters(t *testing.T) {
	// Multiple goroutines writing concurrently; all data should be drained.
	acc := newBatchAccumulator(4096)

	const writers = 10
	const bytesPerWriter = 1000
	var wg sync.WaitGroup
	var totalWritten atomic.Int64

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := bytes.Repeat([]byte("w"), bytesPerWriter)
			n, err := acc.Write(data)
			if err != nil {
				return
			}
			totalWritten.Add(int64(n))
		}()
	}

	// Drain in another goroutine.
	var totalDrained int64
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for {
			chunk, err := acc.Drain()
			if err != nil {
				return
			}
			totalDrained += int64(len(chunk))
		}
	}()

	wg.Wait()
	acc.Close()
	<-drainDone

	if totalDrained != totalWritten.Load() {
		t.Fatalf("drained %d bytes, wrote %d bytes", totalDrained, totalWritten.Load())
	}
}

func TestBatchAccumulatorDrainBlocks(t *testing.T) {
	// Drain should block until data is available.
	acc := newBatchAccumulator(1024)

	drained := make(chan []byte, 1)
	go func() {
		chunk, _ := acc.Drain()
		drained <- chunk
	}()

	// Verify Drain is blocking.
	select {
	case <-drained:
		t.Fatal("Drain should block on empty buffer")
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	// Write data to unblock.
	acc.Write([]byte("hello"))

	select {
	case chunk := <-drained:
		if !bytes.Equal(chunk, []byte("hello")) {
			t.Fatalf("got %q, want %q", chunk, "hello")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Drain should have returned after Write")
	}

	acc.Close()
}

func TestBatchAccumulatorDataIntegrity(t *testing.T) {
	// Write a known pattern, drain all chunks, verify reassembled data.
	const totalSize = 100_000
	const maxBatch = 4096
	acc := newBatchAccumulator(maxBatch)

	// Generate deterministic data.
	original := make([]byte, totalSize)
	for i := range original {
		original[i] = byte(i % 251) // prime to avoid alignment patterns
	}

	// Write in variable-size chunks.
	go func() {
		off := 0
		sizes := []int{1, 7, 100, 1000, 3000, 8192, 500}
		for off < totalSize {
			sz := sizes[off%len(sizes)]
			if off+sz > totalSize {
				sz = totalSize - off
			}
			if _, err := acc.Write(original[off : off+sz]); err != nil {
				return
			}
			off += sz
		}
		acc.Close()
	}()

	// Drain and reassemble.
	var reassembled []byte
	for {
		chunk, err := acc.Drain()
		if err != nil {
			break
		}
		if len(chunk) > maxBatch {
			t.Fatalf("chunk size %d exceeds maxBatch %d", len(chunk), maxBatch)
		}
		reassembled = append(reassembled, chunk...)
	}

	if !bytes.Equal(reassembled, original) {
		t.Fatalf("data integrity check failed: got %d bytes, want %d", len(reassembled), totalSize)
	}
}
