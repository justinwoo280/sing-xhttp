package xhttp

import (
	"io"
	"sync"
)

// batchAccumulator is a thread-safe write buffer that accumulates multiple
// small Write calls and serves them as larger chunks via Drain.
//
// Motivation: the original io.Pipe design provides no batching — each
// conn.Write blocks until the uploader's Read consumes it, producing one
// POST per Write call. With batchAccumulator, multiple small Writes are
// merged into fewer, larger POSTs, dramatically improving throughput.
//
// Flow: upper-layer conn.Write → batchAccumulator.Write → buffer →
//
//	batchAccumulator.Drain → runPacketUploader goroutine → sendOnePost
type batchAccumulator struct {
	mu       sync.Mutex
	cond     *sync.Cond
	data     []byte
	maxBatch int  // maximum bytes returned per Drain call
	closed   bool
	writeErr error // error returned to subsequent Write calls after CloseWithError
}

func newBatchAccumulator(maxBatch int) *batchAccumulator {
	if maxBatch < 1 {
		maxBatch = 1
	}
	b := &batchAccumulator{
		data:     make([]byte, 0, maxBatch),
		maxBatch: maxBatch,
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Write appends p to the internal buffer. If the buffer already holds
// >= maxBatch bytes, Write blocks until Drain consumes data, providing
// natural backpressure to the upper layer.
//
// After Close or CloseWithError, Write returns io.ErrClosedPipe (or the
// error passed to CloseWithError).
func (b *batchAccumulator) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Backpressure: block when buffer is full. We allow up to maxBatch
	// bytes before blocking, so one chunk can be accumulated while the
	// previous chunk is being sent.
	for len(b.data) >= b.maxBatch && !b.closed {
		b.cond.Wait()
	}
	if b.closed {
		if b.writeErr != nil {
			return 0, b.writeErr
		}
		return 0, io.ErrClosedPipe
	}
	b.data = append(b.data, p...)
	b.cond.Broadcast()
	return len(p), nil
}

// Drain returns up to maxBatch bytes of accumulated data, removing them
// from the buffer. It blocks until data is available or the accumulator
// is closed. After Close, Drain returns any remaining buffered data
// before returning io.EOF.
func (b *batchAccumulator) Drain() ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for len(b.data) == 0 && !b.closed {
		b.cond.Wait()
	}
	if len(b.data) == 0 {
		return nil, io.EOF
	}

	n := len(b.data)
	if n > b.maxBatch {
		n = b.maxBatch
	}
	chunk := make([]byte, n)
	copy(chunk, b.data[:n])

	// Shift remaining data forward.
	remaining := copy(b.data, b.data[n:])
	b.data = b.data[:remaining]

	b.cond.Broadcast()
	return chunk, nil
}

// Close signals that no more data will be written. Subsequent Write calls
// return io.ErrClosedPipe. Drain will return remaining buffered data
// before returning io.EOF. Close is idempotent.
func (b *batchAccumulator) Close() error {
	return b.CloseWithError(nil)
}

// CloseWithError is like Close but causes subsequent Write calls to return
// err (or io.ErrClosedPipe if err is nil). If the accumulator is already
// closed, CloseWithError is a no-op (preserving the original error).
func (b *batchAccumulator) CloseWithError(err error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	b.writeErr = err
	b.cond.Broadcast()
	return nil
}
