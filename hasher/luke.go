package hasher

import (
	"hash"

	luke "lukechampine.com/blake3"
)

type LukeHasher struct {
	hasher hash.Hash
}

func NewLukeHasher() Hasher {
	return &LukeHasher{hasher: luke.New(32, nil)}
}

func (s *LukeHasher) Hash(data []byte) []byte {
	x := luke.Sum256(data)
	return x[:]
}

func (s *LukeHasher) Hasher() hash.Hash {
	return s.hasher
}
