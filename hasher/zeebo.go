package hasher

import (
	"hash"

	zeebo "github.com/zeebo/blake3"
)

type ZeeboHasher struct {
	hasher hash.Hash
}

func NewZeeboHasher() Hasher {
	return &ZeeboHasher{hasher: zeebo.New()}
}

func (s *ZeeboHasher) Hash(data []byte) []byte {
	x := zeebo.Sum256(data)
	return x[:]
}

func (s *ZeeboHasher) Hasher() hash.Hash {
	return s.hasher
}
