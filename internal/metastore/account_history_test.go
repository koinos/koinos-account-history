package account

import (
	"errors"
	"io/ioutil"
	"os"

	"github.com/dgraph-io/badger"
)

const (
	MapBackendType    = 0
	BadgerBackendType = 1
)

var backendTypes = [...]int{MapBackendType, BadgerBackendType}

func NewBackend(backendType int) Backend {
	var backend Backend
	switch backendType {
	case MapBackendType:
		backend = NewMapBackend()
		break
	case BadgerBackendType:
		dirname, err := ioutil.TempDir(os.TempDir(), "metastore-test-*")
		if err != nil {
			panic("unable to create temp directory")
		}
		opts := badger.DefaultOptions(dirname)
		backend = NewBadgerBackend(opts)
		break
	default:
		panic("unknown backend type")
	}
	return backend
}

func CloseBackend(b interface{}) {
	switch t := b.(type) {
	case *MapBackend:
		break
	case *BadgerBackend:
		t.Close()
		break
	default:
		panic("unknown backend type")
	}
}

type ErrorBackend struct {
}

func (backend *ErrorBackend) Reset() error {
	return nil
}

// Put returns an error
func (backend *ErrorBackend) Put(key []byte, value []byte) error {
	return errors.New("Error on put")
}

// Get gets an error
func (backend *ErrorBackend) Get(key []byte) ([]byte, error) {
	return nil, errors.New("Error on get")
}

type BadBackend struct {
}

func (backend *BadBackend) Reset() error {
	return nil
}

// Put returns an error
func (backend *BadBackend) Put(key []byte, value []byte) error {
	return nil
}

// Get gets an error
func (backend *BadBackend) Get(key []byte) ([]byte, error) {
	return []byte{0, 0, 255, 255, 255, 255, 255}, nil
}

type LongBackend struct {
}

func (backend *LongBackend) Reset() error {
	return nil
}

// Put returns an error
func (backend *LongBackend) Put(key []byte, value []byte) error {
	return nil
}

// Get gets an error
func (backend *LongBackend) Get(key []byte) ([]byte, error) {
	return []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nil
}
