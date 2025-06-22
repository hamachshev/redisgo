package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file   *os.File
	reader *bufio.Reader
	mu     sync.Mutex
}

func NewAOF(path string) (*Aof, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{file: file, reader: bufio.NewReader(file)}

	go func() {
		for {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()

			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}
func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.Marshall())

	if err != nil {
		return err
	}

	return nil
}

func (aof *Aof) Read(callback func(v Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	resp := NewResp(aof.reader)

	for {
		v, err := resp.Read()

		if err == nil {
			callback(v)
			continue
		}
		if err == io.EOF {
			break
		}
		return err
	}
	return nil
}
