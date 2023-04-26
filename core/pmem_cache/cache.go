package pmem_cache

//#cgo CFLAGS: -I.
//#cgo LDFLAGS: -L${SRCDIR} -lvmemcache_wrapper -lvmemcache
//#include <stdlib.h>
//#include "vmemcache_wrapper.h"
//extern void on_evict(VMEMcache*, void*, size_t, void*);
import "C"
import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

type PmemCache struct {
	cache         *C.VMEMcache
	pmemWriteLock *sync.Mutex
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
var currentLock sync.Mutex

func registerPmemCache(pmemCache *PmemCache) error {
	currentLock.Lock()
	defer currentLock.Unlock()
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
	pmemOnEvict         = metrics.NewRegisteredMeter("core/pmem_cache/on_evict", nil)
)

func GetPmemCache() *PmemCache {
	return pmemCacheCurrent
}

func NewPmemcache() *PmemCache {
	pmemNewMeter.Mark(1)
	log.Info("PmemCache NewPmemcache()")
	// FIXME: New和Open
	if pmemCacheCurrent != nil {
		log.Info("pmemCacheCurrent!=nil, do pmemCacheCurrent.Close()")
		pmemCacheCurrent.Close()
	}
	//TODO: modify the configurations
	path := "/mnt/pmem0/ljy/test"
	path_c := C.CString(path)
	cache_size := int64(1024 * 1024 * 256 * 1) //1GB
	// cache := C.wrapper_vmemcache_new(path_c, C.ulong(cache_size), (*C.vmemcache_on_miss)(C.on_miss))
	cache := C.wrapper_vmemcache_new(path_c, C.ulong(cache_size), (*C.vmemcache_on_evict)(C.on_evict))
	if cache == nil {
		return nil
	}
	pmemCache := &PmemCache{
		cache:         cache,
		pmemWriteLock: &sync.Mutex{},
	}
	err := registerPmemCache(pmemCache)
	if err != nil {
		log.Error(err.Error())
	}
	return pmemCache
}

// always return nil
func (pmemCache *PmemCache) Close() error {
	currentLock.Lock()
	defer currentLock.Unlock()
	pmemCloseMeter.Mark(1)
	log.Info("PmemCache Close()")
	if pmemCacheCurrent != pmemCache {
		log.Error("pmemCacheCurrent!=pmemCache in Close()")
	}
	pmemCacheCurrent = nil
	C.wrapper_vmemcache_delete(pmemCache.cache)
	return nil
}

//export on_evict
func on_evict(cache *C.VMEMcache, key unsafe.Pointer, k_size C.ulong, args unsafe.Pointer) {
	pmemOnEvict.Mark(1)
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

var (
	pmemUpdateMeter     = metrics.NewRegisteredMeter("core/pmem_cache/update", nil)
	pmemWriteCountMeter = metrics.NewRegisteredMeter("core/rawdb/accessors_trie/pmem_write_count", nil)
)

func (pmemCache *PmemCache) Put(key []byte, value []byte) error {
	// test Update
	// pmemWriteCountMeter.Mark(1)
	// if tmp := get(pmemCache.cache, key); tmp != nil {
	// 	pmemUpdateMeter.Mark(1)
	// 	fmt.Println("PmemCache Update!")
	// }

	pmemCache.pmemWriteLock.Lock()
	defer pmemCache.pmemWriteLock.Unlock()
	// fmt.Println("put.key: ", key)
	// fmt.Println("put.value: ", value)
	key_c := C.CBytes(key)
	value_c := C.CBytes(value)
	defer C.free(unsafe.Pointer(key_c))
	defer C.free(unsafe.Pointer(value_c))
	// fmt.Println("put._Ctype_ulong(len(key)): ", C.ulong(len(key)), " put._Ctype_ulong(len(value)): ", C.ulong(len(value)))
	tmp := int(C.wrapper_vmemcache_put(pmemCache.cache, key_c, C.ulong(len(key)), value_c, C.ulong(len(value))))
	if tmp == 0 {
		// if testV, _ := pmemCache.Get(key); !bytes.Equal(testV, value) {
		// 	pmemPutInconsistent.Mark(1)
		// 	fmt.Println("Pmem Put Inconsistent:")
		// 	fmt.Println("len(key): ", len(key), "cap(key): ", cap(key), "key: ", key)
		// 	fmt.Println("len(value): ", len(value), "cap(value): ", cap(value), "value: ", value)
		// 	fmt.Println("len(testV): ", len(testV), "cap(testV): ", cap(testV), "testV: ", testV)
		// }
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
	pmemCache.pmemWriteLock.Lock()
	defer pmemCache.pmemWriteLock.Unlock()
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

func (pmemCache *PmemCache) NewPmemReplayerWriter() *PmemReplayerWriter {
	return &PmemReplayerWriter{
		pmemWriter: pmemCache,
	}
}

type PmemReplayerWriter struct {
	pmemWriter *PmemCache
}

func (replayer *PmemReplayerWriter) Put(key, value []byte) error {
	return replayer.pmemWriter.Put(key, value)
}

func (replayer *PmemReplayerWriter) Delete(key []byte) error {
	return replayer.pmemWriter.Delete(key)
}
