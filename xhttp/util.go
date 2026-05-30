package xhttp

import (
	cryptoRand "crypto/rand"
	"encoding/binary"
	"math/rand"
)

// rangeRand returns a uniformly random int32 in [r.From, r.To].
func rangeRand(r Range) int32 {
	if r.To <= r.From {
		return r.From
	}
	var b [4]byte
	_, _ = cryptoRand.Read(b[:])
	n := int32(binary.LittleEndian.Uint32(b[:]) >> 1) // non-negative
	return r.From + n%(r.To-r.From+1)
}

// randomSeed returns a non-cryptographic int32, used for misc jitter.
func randomSeed() int32 { return int32(rand.Int31()) }
