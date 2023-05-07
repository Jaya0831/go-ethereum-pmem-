package ethdb

import "io"

type PmemReader interface {
	Pmem_Has(key []byte) (bool, error)

	Pmem_Get(key []byte) ([]byte, error)
}

type PmemWriter interface {
	Pmem_Put(key []byte, value []byte) error

	Pmem_Delete(key []byte) error
}

type PmemBatch interface {
	KeyValueWriter
	ValueSize() int
	Write() error
	Reset()
}

type PmemBatcher interface {
	NewPmemBatch() PmemBatch
}

type PmemCache interface {
	PmemReader
	PmemWriter
	io.Closer
	PmemBatcher
}
