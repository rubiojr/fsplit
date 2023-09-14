package fsplit

import (
	"bufio"
	"sync"

	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/restic/chunker"
	"github.com/rubiojr/fsplit/hasher"
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

type Chunk struct {
	Position uint64
	Size     uint64
	Data     []byte
}

type Splitter interface {
	ReadManifest(path string) (*Manifest, error)
	Assemble(mf *Manifest, dst io.Writer) error
	Split(source io.Reader, dstDir string) (string, error)
	StreamChunks(source io.Reader, chunks chan *Chunk, done chan string) error
	SplitParallel(source io.Reader, dstDir string) (string, error)
	ChunkerPolynomial() chunker.Pol
	SetHasher(h hasher.Hasher)
}

type splitter struct {
	MinSize    uint // in Bytes
	MaxSize    uint // in Bytes
	Polynomial chunker.Pol
	hasher     hasher.Hasher
}

func (s *splitter) SetHasher(h hasher.Hasher) {
	s.hasher = h
}

func (s *splitter) SplitParallel(source io.Reader, dstDir string) (string, error) {
	ch := make(chan *Chunk, 10)
	var totalSize int64
	chunks := make([]string, 1024)
	var count int
	done := make(chan string)

	go func() {
		s.StreamChunks(source, ch, done)
	}()

	wg := sync.WaitGroup{}
	for c := range ch {
		wg.Add(1)
		go func(c *Chunk) {
			count++
			csum := s.hasher.Hash(c.Data)
			tfile := fmt.Sprintf("%s/%02x.chk", dstDir, csum)
			totalSize = totalSize + int64(c.Size)
			chunks[c.Position] = fmt.Sprintf("%d %02x", c.Size, csum)
			err := os.WriteFile(tfile, c.Data, 0644)
			wg.Done()
			if err != nil {
				panic(err)
			}
		}(c)
	}

	wg.Wait()
	fileHash := <-done
	mfpath := fmt.Sprintf("%s/%s.manifest", dstDir, fileHash)
	mf, err := os.Create(mfpath)
	if err != nil {
		return fileHash, err
	}
	defer mf.Close()

	_, err = mf.WriteString(fmt.Sprintf("%s %d %s\n", s.Polynomial, totalSize, fileHash))
	if err != nil {
		return fileHash, err
	}

	for _, c := range chunks[:count] {
		if c == "" {
			panic("empty")
		}
		_, err = mf.Write([]byte(c + "\n"))
		if err != nil {
			return fileHash, err
		}
	}

	return fileHash, nil
}

func (s *splitter) StreamChunks(source io.Reader, ch chan *Chunk, done chan string) error {
	h := s.hasher.Hasher()
	tee := io.TeeReader(source, h)

	chnkr := chunker.NewWithBoundaries(tee, s.Polynomial, s.MinSize, s.MaxSize)
	buf := make([]byte, maxSize)

	var pos uint64
	for {
		chunk, err := chnkr.Next(buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		c := &Chunk{
			Position: pos,
			Size:     uint64(chunk.Length),
			Data:     make([]byte, chunk.Length),
		}
		copy(c.Data, chunk.Data)
		pos++
		ch <- c
	}
	close(ch)

	fileHash := fmt.Sprintf("%x", h.Sum(nil))
	done <- fileHash

	return nil
}

func (s *splitter) ChunkerPolynomial() chunker.Pol {
	return s.Polynomial
}

func DefaultSplitter() Splitter {
	cp, err := chunker.RandomPolynomial()
	if err != nil {
		panic(err)
	}
	return &splitter{MinSize: minSize, MaxSize: maxSize, Polynomial: cp, hasher: DefaultHasher()}
}

func DefaultHasher() hasher.Hasher {
	return hasher.NewZeeboHasher()
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
			_, err = fmt.Sscanf(line, "0x%x %d %s", &mf.Polynomial, &mf.Size, &mf.Hash)
			if err != nil {
				return nil, err
			}
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
	h := s.hasher.Hasher()
	tee := io.TeeReader(source, h)

	chnkr := chunker.NewWithBoundaries(tee, s.Polynomial, s.MinSize, s.MaxSize)
	buf := make([]byte, maxSize)

	chunks := []string{}
	for {
		chunk, err := chnkr.Next(buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			return "", err
		}

		totalSize = totalSize + int64(chunk.Length)
		csum := s.hasher.Hash(chunk.Data)
		chunks = append(chunks, fmt.Sprintf("%d %02x", chunk.Length, csum))
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

	for _, c := range chunks {
		_, err = mf.WriteString(c + "\n")
		if err != nil {
			return fileHash, err
		}
	}

	return fileHash, nil
}
