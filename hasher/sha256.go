package hasher

import (
	"crypto/sha256"
	"hash"
)

type Sha256Hasher struct {
	hasher hash.Hash
}

func NewSha256Hasher() Hasher {
	return &Sha256Hasher{hasher: sha256.New()}
}

func (s *Sha256Hasher) Hash(data []byte) []byte {
	x := sha256.Sum256(data)
	return x[:]
}

func (s *Sha256Hasher) Hasher() hash.Hash {
	return s.hasher
}
