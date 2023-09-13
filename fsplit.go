package fsplit

import (
	"bufio"
	//"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/sha256-simd"

	"github.com/restic/chunker"
)

const minSize = 128 * 1024 * 1024
const maxSize = 256 * 1024 * 1024

type Manifest struct {
	Path       string
	Size       int64
	Hash       string
	Chunks     []string
	Polynomial chunker.Pol
}

type Splitter interface {
	ReadManifest(path string) (*Manifest, error)
	Assemble(mf *Manifest, dst io.Writer) error
	Split(source io.Reader, dstDir string) (string, error)
}

type splitter struct {
	MinSize    uint // in Bytes
	MaxSize    uint // in Bytes
	Polynomial chunker.Pol
}

func DefaultSplitter() Splitter {
	cp, err := chunker.RandomPolynomial()
	if err != nil {
		panic(err)
	}
	return &splitter{MinSize: minSize, MaxSize: maxSize, Polynomial: cp}
}

func (s *splitter) ReadManifest(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	firstLine := true
	chunks := []string{}
	mf := &Manifest{Path: path}

	for scanner.Scan() {
		line := scanner.Text()

		if firstLine {
			_, err = fmt.Sscanf(line, "%s %d %s", &mf.Polynomial, &mf.Size, &mf.Hash)
			firstLine = false
			continue
		}

		sha := strings.Split(line, " ")[1]
		chunks = append(chunks, sha)
	}

	mf.Chunks = chunks
	return mf, scanner.Err()
}

func (s *splitter) Assemble(mf *Manifest, dst io.Writer) error {
	basePath := filepath.Dir(mf.Path)

	f, err := os.Open(mf.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, sha := range mf.Chunks {
		chk, err := os.Open(filepath.Join(basePath, fmt.Sprintf("%s.chk", sha)))
		if err != nil {
			return err
		}

		_, err = io.Copy(dst, chk)
		if err != nil {
			chk.Close()
			return err
		}
		chk.Close()
	}

	return nil
}

func (s *splitter) Split(source io.Reader, dstDir string) (string, error) {
	if _, err := os.Stat(dstDir); os.IsNotExist(err) {
		return "", fmt.Errorf("chunks destination directory does not exist")
	}

	var totalSize int64
	var err error
	h := sha256.New()
	tee := io.TeeReader(source, h)

	chnkr := chunker.NewWithBoundaries(tee, s.Polynomial, s.MinSize, s.MaxSize)
	buf := make([]byte, maxSize)

	chunks := map[string]uint{}
	for {
		chunk, err := chnkr.Next(buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			return "", err
		}

		totalSize = totalSize + int64(chunk.Length)
		csum := sha256.Sum256(chunk.Data)
		chunks[fmt.Sprintf("%02x", csum)] = chunk.Length
		tfile := fmt.Sprintf("%s/%02x.chk", dstDir, csum)
		os.WriteFile(tfile, chunk.Data, 0644)
	}

	fileHash := fmt.Sprintf("%x", h.Sum(nil))

	mf, err := os.Create(fmt.Sprintf("%s/%s.manifest", dstDir, fileHash))
	if err != nil {
		return fileHash, err
	}
	defer mf.Close()

	_, err = mf.WriteString(fmt.Sprintf("%s %d %s\n", s.Polynomial, totalSize, fileHash))
	if err != nil {
		return fileHash, err
	}

	for sha, size := range chunks {
		_, err = mf.WriteString(fmt.Sprintf("%d %s\n", size, sha))
		if err != nil {
			return fileHash, err
		}
	}

	return fileHash, nil
}
