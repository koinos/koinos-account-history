package account

import (
	"fmt"
	"sync"

	"github.com/koinos/koinos-proto-golang/koinos/contract_meta_store"
	"google.golang.org/protobuf/proto"
)

// MetaStore contains a backend object and handles requests
type MetaStore struct {
	backend Backend
	rwmutex sync.RWMutex
}

// NewAccountHistory creates a new MetaStore wrapping the provided backend
func NewAccountHistory(backend Backend) *MetaStore {
	return &MetaStore{backend: backend}
}

// AddMeta adds the given metadata at the given contract id
func (handler *MetaStore) AddMeta(contractID []byte, meta *contract_meta_store.ContractMetaItem) error {
	handler.rwmutex.Lock()
	defer handler.rwmutex.Unlock()

	itemBytes, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("%w, %v", ErrSerialization, err)
	}

	err = handler.backend.Put(contractID, itemBytes)
	if err != nil {
		return fmt.Errorf("%w, %v", ErrBackend, err)
	}

	return nil
}

// GetContractMeta returns transactions by transaction ID
func (handler *MetaStore) GetContractMeta(contractID []byte) (*contract_meta_store.ContractMetaItem, error) {
	handler.rwmutex.RLock()
	defer handler.rwmutex.RUnlock()

	itemBytes, err := handler.backend.Get(contractID)
	if err != nil {
		return nil, fmt.Errorf("%w, %v", ErrBackend, err)
	}

	if len(itemBytes) != 0 {
		item := &contract_meta_store.ContractMetaItem{}
		if err := proto.Unmarshal(itemBytes, item); err != nil {
			return nil, fmt.Errorf("%w, %v", ErrDeserialization, err)
		}

		return item, nil
	}

	return nil, nil
}
