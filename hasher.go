package fsplit

import (
	"crypto/sha256"
	"hash"

	zeebo "github.com/zeebo/blake3"
	luke "lukechampine.com/blake3"
)

type Hasher interface {
	Hash([]byte) []byte
	Hasher() hash.Hash
}

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
