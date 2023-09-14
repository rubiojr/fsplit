package hasher

import (
	"hash"
)

type Hasher interface {
	Hash([]byte) []byte
	Hasher() hash.Hash
}
