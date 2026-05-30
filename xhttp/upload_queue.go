package xhttp

// uploadQueue is a specialized priorityqueue + channel to reorder generic
// packets by a sequence number. Packets may arrive out of order from
// concurrent POST requests; the queue buffers and reorders them before
// the server reads them in sequence.

import (
	"container/heap"
	"io"
	"sync"

	E "github.com/sagernet/sing/common/exceptions"
)

type packet struct {
	payload []byte
	seq     uint64
}

type uploadQueue struct {
	mu            sync.Mutex
	pushed        chan packet
	heap          uploadHeap
	nextSeq       uint64
	closed        bool
	maxBuffered   int
}

func newUploadQueue(maxBuffered int) *uploadQueue {
	if maxBuffered <= 0 {
		maxBuffered = 30
	}
	return &uploadQueue{
		pushed:      make(chan packet, maxBuffered),
		maxBuffered: maxBuffered,
	}
}

func (q *uploadQueue) Push(p packet) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return E.New("upload queue closed")
	}
	q.mu.Unlock()
	// Blocking send provides natural back-pressure: when the queue is full,
	// the server's ServeHTTP POST handler waits, which in turn flow-controls
	// the H2 stream, which back-pressures the client's Write().
	q.pushed <- p
	return nil
}

func (q *uploadQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	close(q.pushed)
	return nil
}

// Read implements io.Reader. It returns reassembled bytes in seq order.
func (q *uploadQueue) Read(b []byte) (int, error) {
	if len(q.heap) == 0 {
		p, ok := <-q.pushed
		if !ok {
			return 0, io.EOF
		}
		heap.Push(&q.heap, p)
	}
	for len(q.heap) > 0 {
		p := heap.Pop(&q.heap).(packet)
		if p.seq == q.nextSeq {
			n := copy(b, p.payload)
			if n < len(p.payload) {
				p.payload = p.payload[n:]
				heap.Push(&q.heap, p)
			} else {
				q.nextSeq = p.seq + 1
			}
			return n, nil
		}
		if p.seq > q.nextSeq {
			if len(q.heap) > q.maxBuffered {
				return 0, E.New("reassembly buffer overflow")
			}
			heap.Push(&q.heap, p)
			next, ok := <-q.pushed
			if !ok {
				return 0, io.EOF
			}
			heap.Push(&q.heap, next)
			continue
		}
		// p.seq < nextSeq: stale duplicate, drop and continue.
	}
	return 0, nil
}

type uploadHeap []packet

func (h uploadHeap) Len() int            { return len(h) }
func (h uploadHeap) Less(i, j int) bool  { return h[i].seq < h[j].seq }
func (h uploadHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *uploadHeap) Push(x any)         { *h = append(*h, x.(packet)) }
func (h *uploadHeap) Pop() any           { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }
