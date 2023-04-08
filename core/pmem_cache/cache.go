package pmem_cache

//#cgo CFLAGS: -I.
//#cgo LDFLAGS: -L${SRCDIR} -lvmemcache_wrapper -lvmemcache
//#include <stdlib.h>
//#include "vmemcache_wrapper.h"
//extern void on_miss(VMEMcache*, void*, size_t, void*);
import "C"
import (
	"fmt"
	"sync"
	"unsafe"
)

type PmemCache struct {
	cache *C.VMEMcache
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

func NewPmemcache() *PmemCache {
	// FIXME: New和Open
	if pmemCacheCurrent != nil {
		fmt.Println("pmemCacheCurrent!=nil, do cleanPmemCache()")
	}
	cleanPmemCache()
	//TODO: modify the configurations
	path := "/dev/dax0.1"
	path_c := C.CString(path)
	cache_size := int64(1024 * 1024 * 1024 * 10) //10GB
	// cache := C.wrapper_vmemcache_new(path_c, C.ulong(cache_size), (*C.vmemcache_on_miss)(C.on_miss))
	cache := C.wrapper_vmemcache_new(path_c, C.ulong(cache_size), nil)
	if cache == nil {
		return nil
	}
	pmemCache := &PmemCache{
		cache: cache,
	}
	err := registerPmemCache(pmemCache)
	if err != nil {
		fmt.Println(err.Error())
	}
	return pmemCache
}

// always return nil
func (pmemCache *PmemCache) Close() error {
	if pmemCacheCurrent != pmemCache {
		fmt.Println("pmemCacheCurrent!=pmemCache in Close()")
	}
	poolLock.Lock()
	defer poolLock.Unlock()
	pmemCacheCurrent = nil
	C.wrapper_vmemcache_delete(pmemCache.cache)
	return nil
}

//export on_miss
func on_miss(cache *C.VMEMcache, key unsafe.Pointer, k_size C.ulong, args unsafe.Pointer) {
	// fmt.Print("---on_miss!\n")
	value := []byte{'n', 'u', 'l', 'l'}
	value_c := C.CBytes(value)
	defer C.free(value_c)
	C.wrapper_vmemcache_put(cache, key, value_c)
	// fmt.Print("---finish on_miss\n")
}

func get(cache *C.VMEMcache, key []byte) []byte {
	// fmt.Print("--get ", key, "\n")
	key_c := C.CBytes(key)
	defer C.free(key_c)

	value_c := C.wrapper_vmemcache_get(cache, key_c)
	// fmt.Printf("--value_c in get: %s\n", C.GoString(value_c))
	if value_c == nil {
		return nil
	}
	return []byte(C.GoString(value_c))
}

// TODO: error is always nil
func (pmemCache *PmemCache) Get(key []byte) ([]byte, error) {
	return get(pmemCache.cache, key), nil
}

func (pmemCache *PmemCache) Put(key []byte, value []byte) error {
	key_c := C.CBytes(key)
	value_c := C.CBytes(value)
	defer C.free(key_c)
	defer C.free(value_c)
	tmp := int(C.wrapper_vmemcache_put(pmemCache.cache, key_c, value_c))
	if tmp == 0 {
		return nil
	} else {
		return NewPmemError("Pmem Put Error")
	}
}

// Returns 0 if an entry has been deleted, -1 otherwise.
func (pmemCache *PmemCache) Delete(key []byte) error {
	key_c := C.CBytes(key)
	defer C.free(key_c)
	tmp := int(C.wrapper_vmemcache_evict(pmemCache.cache, key_c))
	if tmp == 0 {
		return nil
	} else {
		return NewPmemError("Pmem Delete Error")
	}
}

func (pmemCache *PmemCache) Has(key []byte) (bool, error) {
	key_c := C.CBytes(key)
	defer C.free(key_c)
	tmp := int(C.wrapper_vmemcache_exists(pmemCache.cache, key_c))
	if tmp == -1 {
		return false, nil
	} else if tmp == -2 {
		return false, NewPmemError("Pmem Has Error")
	} else {
		return true, nil
	}
}
