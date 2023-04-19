package pmem_cache

//#cgo CFLAGS: -I.
//#cgo LDFLAGS: -L${SRCDIR} -lvmemcache_wrapper -lvmemcache
//#include <stdlib.h>
//#include "vmemcache_wrapper.h"
//extern void on_miss(VMEMcache*, void*, size_t, void*);
import "C"
import (
	"bytes"
	"fmt"
	"sync"
	"unsafe"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

type PmemCache struct {
	cache *C.VMEMcache
}

type PmemCacheConfig struct {
	Path string
	Size int64
}

type PmemError struct {
	msg string
}

func NewPmemError(s string) *PmemError {
	return &PmemError{msg: s}
}

func (pmemError *PmemError) Error() string {
	return pmemError.msg
}

var (
	poolLock      sync.Mutex
	pmemCachePool [2]*PmemCache
)

const (
	TrieCache  = iota //0
	ChainCache = iota //1
	CacheNum   = 2
)

// cleanPmemCache closes the current Pmem Cache according to the given cacheType, and clean its record in the PmemCachePool
func cleanPmemCache(cacheType int) {
	poolLock.Lock()
	defer poolLock.Unlock()
	if pmemCachePool[cacheType] != nil {
		pmemCachePool[cacheType].Close(cacheType)
		pmemCachePool[cacheType] = nil
	}
}

// registerPmemCache registers the pointer of the given pmemCache in PmemCachePool
func registerPmemCache(cacheType int, pmemCache *PmemCache) error {
	poolLock.Lock()
	defer poolLock.Unlock()
	if pmemCachePool[cacheType] != nil {
		return NewPmemError("RegisterPmemCache Error")
	}
	pmemCachePool[cacheType] = pmemCache
	return nil
}

var (
	pmemNewMeter        = metrics.NewRegisteredMeter("core/pmem_cache/new", nil)
	pmemCloseMeter      = metrics.NewRegisteredMeter("core/pmem_cache/close", nil)
	pmemPutError        = metrics.NewRegisteredMeter("core/pmem_cache/put_error", nil)
	pmemPutInconsistent = metrics.NewRegisteredMeter("core/pmem_cache/put_inconsistent", nil)
)

// NewPmemCache creates a new cache according to the cacheType and config information, and return nil if an error occurs
func NewPmemCache(cacheType int, config *PmemCacheConfig) *PmemCache {
	pmemNewMeter.Mark(1)
	log.Info("PmemCache NewPmemcache()")
	// FIXME: New和Open(改cache的tmpfile)
	if pmemCachePool[cacheType] != nil {
		log.Info("pmemCachePool[", cacheType, "]!=nil, do cleanPmemCache()")
	}
	cleanPmemCache(cacheType)
	// path := "/dev/dax0.1"
	path_c := C.CString(config.Path)
	defer C.free(unsafe.Pointer(path_c))
	cache := C.wrapper_vmemcache_new(path_c, C.ulong(config.Size), nil)
	if cache == nil {
		log.Error("wrapper_vmemcache_new fails")
		return nil
	}
	pmemCache := &PmemCache{
		cache: cache,
	}
	err := registerPmemCache(cacheType, pmemCache)
	if err != nil {
		log.Error(err.Error())
	}
	return pmemCache
}

// Close a PmemCache according to its `pointer` and `cacheType`. If the current Pmemcache pointer of `cacheType` is no equal to
// `pointer`, an error is triggered.
func (pmemCache *PmemCache) Close(cacheType int) {
	pmemCloseMeter.Mark(1)
	log.Info("PmemCache Close()")
	if pmemCachePool[cacheType] != pmemCache {
		log.Error("pmemCachePool[", cacheType, "]!=pmemCache in Close()")
	}
	poolLock.Lock()
	defer poolLock.Unlock()
	pmemCachePool[cacheType] = nil
	C.wrapper_vmemcache_delete(pmemCache.cache)
}

//export on_miss
func on_miss(cache *C.VMEMcache, key unsafe.Pointer, k_size C.ulong, args unsafe.Pointer) {

}

func get(cache *C.VMEMcache, key []byte) []byte {
	// fmt.Println("get.key: ", key)
	key_c := C.CBytes(key)
	defer C.free(key_c)

	value_struct := C.wrapper_vmemcache_get(cache, key_c, C.ulong(len(key)))
	if value_struct.buf == nil {
		return nil
	}
	value := C.GoBytes(value_struct.buf, value_struct.len)
	// fmt.Println("get.value: ", value)
	return value
}

// TODO: error is always nil
func (pmemCache *PmemCache) Get(key []byte) ([]byte, error) {
	return get(pmemCache.cache, key), nil
}

func (pmemCache *PmemCache) Put(key []byte, value []byte) error {
	fmt.Println("put.key: ", key)
	fmt.Println("put.value: ", value)
	key_c := C.CBytes(key)
	value_c := C.CBytes(value)
	defer C.free(unsafe.Pointer(key_c))
	defer C.free(unsafe.Pointer(value_c))
	// fmt.Println("put._Ctype_ulong(len(key)): ", C.ulong(len(key)), " put._Ctype_ulong(len(value)): ", C.ulong(len(value)))
	tmp := int(C.wrapper_vmemcache_put(pmemCache.cache, key_c, C.ulong(len(key)), value_c, C.ulong(len(value))))
	if tmp == 0 {
		if testV, _ := pmemCache.Get(key); !bytes.Equal(testV, value) {
			pmemPutInconsistent.Mark(1)
			fmt.Println("Pmem Put Inconsistent:")
			fmt.Println("len(key): ", len(key), "cap(key): ", cap(key), "key: ", key)
			fmt.Println("len(value): ", len(value), "cap(value): ", cap(value), "value: ", value)
			fmt.Println("len(testV): ", len(testV), "cap(testV): ", cap(testV), "testV: ", testV)
		}
		// log.Info("Pmem Put Success")
		return nil
	} else {
		pmemPutError.Mark(1)
		fmt.Println("Pmem Put Error:")
		fmt.Println("len(key): ", len(key), "cap(key): ", cap(key), "key: ", key)
		fmt.Println("len(value): ", len(value), "cap(value): ", cap(value), "value: ", value)
		return NewPmemError("Pmem Put Error")
	}
}

// Returns 0 if an entry has been deleted, -1 otherwise.
func (pmemCache *PmemCache) Delete(key []byte) error {
	key_c := C.CBytes(key)
	defer C.free(key_c)
	tmp := int(C.wrapper_vmemcache_evict(pmemCache.cache, key_c, C.ulong(len(key))))
	if tmp == 0 {
		return nil
	} else {
		return NewPmemError("Pmem Delete Error")
	}
}

func (pmemCache *PmemCache) Has(key []byte) (bool, error) {
	key_c := C.CBytes(key)
	defer C.free(key_c)
	tmp := int(C.wrapper_vmemcache_exists(pmemCache.cache, key_c, C.ulong(len(key))))
	if tmp == -1 {
		return false, nil
	} else if tmp == -2 {
		return false, NewPmemError("Pmem Has Error")
	} else {
		return true, nil
	}
}
