package hasher

import (
	"hash"

	"golang.org/x/crypto/blake2b"
)

type Blake2bHasher struct {
	hasher hash.Hash
}

func NewBlake2bHasher() Hasher {
	h, err := blake2b.New256(nil)
	if err != nil {
		panic(err)
	}
	return &Blake2bHasher{hasher: h}
}

func (s *Blake2bHasher) Hash(data []byte) []byte {
	x := blake2b.Sum256(data)
	return x[:]
}

func (s *Blake2bHasher) Hasher() hash.Hash {
	return s.hasher
}
