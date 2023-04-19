package ethdb

import "io"

type PmemReader interface {
	Pmem_Has(cacheType int, key []byte) (bool, error)

	Pmem_Get(cacheType int, key []byte) ([]byte, error)
}

type PmemWriter interface {
	Pmem_Put(cacheType int, key []byte, value []byte) error

	Pmem_Delete(cacheType int, key []byte) error
}

type PmemCache interface {
	PmemReader
	PmemWriter
	io.Closer
}
