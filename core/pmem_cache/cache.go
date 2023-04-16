package pmem_cache

//#cgo CFLAGS: -I.
//#cgo LDFLAGS: -L${SRCDIR} -lvmemcache_wrapper -lvmemcache
//#include <stdlib.h>
//#include "vmemcache_wrapper.h"
//extern void on_miss(VMEMcache*, void*, size_t, void*);
//extern void on_evict(VMEMcache*, void*, size_t, void*);
//extern void on_evict_dirty(VMEMcache*, void*, size_t, void*);
import "C"
import (
	"bytes"
	"fmt"
	"sync"
	"unsafe"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/syndtr/goleveldb/leveldb"
)

type PmemCache struct {
	under_leveldb *leveldb.DB
	cache         *C.VMEMcache
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

var pmemCacheCurrent *PmemCache = nil
var poolLock sync.Mutex

func cleanPmemCache() error {
	poolLock.Lock()
	defer poolLock.Unlock()
	if pmemCacheCurrent != nil {
		rt := pmemCacheCurrent.Close()
		pmemCacheCurrent = nil
		return rt
	}
	return nil
}
func registerPmemCache(pmemCache *PmemCache) error {
	poolLock.Lock()
	defer poolLock.Unlock()
	if pmemCacheCurrent != nil {
		return NewPmemError("RegisterPmemCache Error")
	}
	pmemCacheCurrent = pmemCache
	return nil
}

var (
	pmemNewMeter        = metrics.NewRegisteredMeter("core/pmem_cache/new", nil)
	pmemCloseMeter      = metrics.NewRegisteredMeter("core/pmem_cache/close", nil)
	pmemPutError        = metrics.NewRegisteredMeter("core/pmem_cache/put_error", nil)
	pmemPutInconsistent = metrics.NewRegisteredMeter("core/pmem_cache/put_inconsistent", nil)

	pmemOnEvict      = metrics.NewRegisteredMeter("core/pmem_cache/on_evict", nil)
	pmemOnEvictDirty = metrics.NewRegisteredMeter("core/pmem_cache/on_evict_dirty", nil)
)

func NewPmemcache(db *leveldb.DB) *PmemCache {
	pmemNewMeter.Mark(1)
	log.Info("PmemCache NewPmemcache()")
	// FIXME: New和Open
	if pmemCacheCurrent != nil {
		log.Info("pmemCacheCurrent!=nil, do cleanPmemCache()")
	}
	cleanPmemCache()
	//TODO: modify the configurations
	path := "/dev/dax0.1"
	path_c := C.CString(path)
	cache_size := int64(1024 * 1024 * 30 * 1) //30MB
	// cache := C.wrapper_vmemcache_new(path_c, C.ulong(cache_size), (*C.vmemcache_on_miss)(C.on_miss))
	cache := C.wrapper_vmemcache_new(path_c, C.ulong(cache_size),
		(*C.vmemcache_on_evict)(C.on_evict), (*C.vmemcache_on_evict_dirty)(C.on_evict_dirty))
	if cache == nil {
		return nil
	}
	pmemCache := &PmemCache{
		cache:         cache,
		under_leveldb: db,
	}
	err := registerPmemCache(pmemCache)
	if err != nil {
		log.Error(err.Error())
	}
	return pmemCache
}

// always return nil
func (pmemCache *PmemCache) Close() error {
	pmemCloseMeter.Mark(1)
	log.Info("PmemCache Close()")
	if pmemCacheCurrent != pmemCache {
		log.Error("pmemCacheCurrent!=pmemCache in Close()")
	}
	poolLock.Lock()
	defer poolLock.Unlock()
	pmemCacheCurrent = nil
	C.wrapper_vmemcache_delete(pmemCache.cache)
	return nil
}

// export on_miss: In our implementation, as wrapper_vmemcache_get will first call vmemcache_exists to ensure the existence of the key,
// on_miss won't be called in the normal situation.
func on_miss(cache *C.VMEMcache, key unsafe.Pointer, k_size C.ulong, args unsafe.Pointer) {
	log.Error("on_miss")
}

//export on_evict
func on_evict(cache *C.VMEMcache, key unsafe.Pointer, k_size C.ulong, args unsafe.Pointer) {
	// TODO: for test, on_evict should be nil during runtime
	pmemOnEvict.Mark(1)
	// fmt.Print("---on_evict!\n")
}

//export on_evict_dirty
func on_evict_dirty(cache *C.VMEMcache, key unsafe.Pointer, k_size C.ulong, args unsafe.Pointer) {
	// TODO: write back to the disk
	pmemOnEvictDirty.Mark(1)
	db := pmemCacheCurrent.under_leveldb
	value_struct := C.wrapper_vmemcache_get(cache, key, C.ulong(k_size))
	if value_struct.buf == nil {
		log.Error("core/pmem_cache.on_evict_dirty: value_struct.buf=nil")
	}
	key_go := C.GoBytes(key, C.int(k_size))
	value_go := C.GoBytes(value_struct.buf, value_struct.len)
	db.Put(key_go, value_go, nil)
	// fmt.Print("---on_evict_dirty!\n")
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

func (pmemCache *PmemCache) Put(key []byte, value []byte, dirty byte) error {
	// fmt.Println("put.key: ", key)
	// fmt.Println("put.value: ", value)
	key_c := C.CBytes(key)
	value_c := C.CBytes(value)
	defer C.free(unsafe.Pointer(key_c))
	defer C.free(unsafe.Pointer(value_c))
	// fmt.Println("put._Ctype_ulong(len(key)): ", C.ulong(len(key)), " put._Ctype_ulong(len(value)): ", C.ulong(len(value)))
	tmp := int(C.wrapper_vmemcache_put(pmemCache.cache, key_c, C.ulong(len(key)), value_c, C.ulong(len(value)), C.char(dirty)))
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
