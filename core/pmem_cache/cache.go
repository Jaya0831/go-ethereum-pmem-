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
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

type PMemError struct {
	msg string
}

func NewPmemError(s string) *PMemError {
	return &PMemError{msg: s}
}
func (pmemError *PMemError) Error() string {
	return pmemError.msg
}

var (
	pmemCacheCurrent *PMemCache = nil
	currentLock      sync.Mutex

	// metrics
	pmemNewMeter      = metrics.NewRegisteredMeter("core/pmem_cache/new", nil)
	pmemCloseMeter    = metrics.NewRegisteredMeter("core/pmem_cache/close", nil)
	pmemPutErrorMeter = metrics.NewRegisteredMeter("core/pmem_cache/put_error", nil)
	// pmemPutInconsistent = metrics.NewRegisteredMeter("core/pmem_cache/put_inconsistent", nil)
	pmemOnEvictMeter = metrics.NewRegisteredMeter("core/pmem_cache/on_evict", nil)
	pmemWriteKVMeter = metrics.NewRegisteredMeter("core/pmem_cache/writeKV", nil)
	pmemDeleteMeter  = metrics.NewRegisteredMeter("core/pmem_cache/delete", nil)
	// pmemUpdateMeter     = metrics.NewRegisteredMeter("core/pmem_cache/update", nil)
	pmemWriteCountMeter = metrics.NewRegisteredMeter("core/pmem_cache/write_count", nil)

	pmemBatchWriteTimer       = metrics.NewRegisteredTimer("core/pmem_cache/batch_write", nil)
	pmemBatchWriteStartMeter  = metrics.NewRegisteredMeter("core/pmem_cache/batch_write_start", nil)
	pmemBatchWriteFinishMeter = metrics.NewRegisteredMeter("core/pmem_cache/batch_write_finish", nil)
)

type PMemCache struct {
	cache         *C.VMEMcache
	pmemWriteLock *sync.Mutex
}

func registerPmemCache(pmemCache *PMemCache) error {
	currentLock.Lock()
	defer currentLock.Unlock()
	if pmemCacheCurrent != nil {
		return NewPmemError("RegisterPmemCache Error")
	}
	pmemCacheCurrent = pmemCache
	return nil
}

func GetPmemCache() *PMemCache {
	return pmemCacheCurrent
}

func NewPmemcache() *PMemCache {
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
	defer C.free(unsafe.Pointer(path_c))
	cache_size := int64(1024 * 1024 * 64 * 1) //1GB
	cache := C.wrapper_vmemcache_new(path_c, C.ulong(cache_size), (*C.vmemcache_on_evict)(C.on_evict))
	if cache == nil {
		return nil
	}
	pmemCache := &PMemCache{
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
func (pmemCache *PMemCache) Close() error {
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
	pmemOnEvictMeter.Mark(1)
}

// TODO: error is always nil
func (pmemCache *PMemCache) Get(key []byte) ([]byte, error) {
	return get(pmemCache.cache, key), nil
}

func get(cache *C.VMEMcache, key []byte) []byte {
	key_c := C.CBytes(key)
	defer C.free(key_c)

	value_struct := C.wrapper_vmemcache_get(cache, key_c, C.ulong(len(key)))
	if value_struct.buf == nil {
		return nil
	}
	defer C.free(value_struct.buf)
	value := C.GoBytes(value_struct.buf, value_struct.len)
	return value
}

func (pmemCache *PMemCache) Put(key []byte, value []byte) error {
	// test Update
	// pmemWriteCountMeter.Mark(1)
	// if tmp := get(pmemCache.cache, key); tmp != nil {
	// 	pmemUpdateMeter.Mark(1)
	// 	fmt.Println("PmemCache Update!")
	// }
	pmemCache.pmemWriteLock.Lock()
	defer pmemCache.pmemWriteLock.Unlock()
	pmemWriteCountMeter.Mark(1)
	pmemWriteKVMeter.Mark(int64(len(key) + len(value)))
	key_c := C.CBytes(key)
	value_c := C.CBytes(value)
	defer C.free(unsafe.Pointer(key_c))
	defer C.free(unsafe.Pointer(value_c))
	tmp := int(C.wrapper_vmemcache_put(pmemCache.cache, key_c, C.ulong(len(key)), value_c, C.ulong(len(value))))
	if tmp == 0 {
		return nil
	} else {
		pmemPutErrorMeter.Mark(1)
		// fmt.Println("Pmem Put Error:")
		// fmt.Println("len(key): ", len(key), "cap(key): ", cap(key), "key: ", key)
		// fmt.Println("len(value): ", len(value), "cap(value): ", cap(value), "value: ", value)
		return NewPmemError("Pmem Put Error")
	}
}

// Returns 0 if an entry has been deleted, -1 otherwise.
func (pmemCache *PMemCache) Delete(key []byte) error {
	pmemCache.pmemWriteLock.Lock()
	defer pmemCache.pmemWriteLock.Unlock()
	pmemDeleteMeter.Mark(1)
	key_c := C.CBytes(key)
	defer C.free(key_c)
	tmp := int(C.wrapper_vmemcache_evict(pmemCache.cache, key_c, C.ulong(len(key))))
	if tmp == 0 {
		return nil
	} else {
		return NewPmemError("Pmem Delete Error")
	}
}

func (pmemCache *PMemCache) Has(key []byte) (bool, error) {
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

func (pmemCache *PMemCache) NewPmemReplayerWriter() *PMemReplayerWriter {
	return &PMemReplayerWriter{
		pmemWriter: pmemCache,
	}
}

type PMemReplayerWriter struct {
	pmemWriter *PMemCache
}

func (replayer *PMemReplayerWriter) Put(key, value []byte) error {
	return replayer.pmemWriter.Put(key, value)
}

func (replayer *PMemReplayerWriter) Delete(key []byte) error {
	return replayer.pmemWriter.Delete(key)
}

const IdealBatchSize = 100 * 1024

type pmemBatch struct {
	batch map[string][]byte
	pmem  *PMemCache
	size  int
}

func (pmemCache *PMemCache) NewPmemBatch() *pmemBatch {
	return &pmemBatch{
		batch: make(map[string][]byte),
		pmem:  pmemCache,
		size:  0,
	}
}

func (b *pmemBatch) Put(key, value []byte) error {
	old_key, ok := b.batch[string(key)]
	if ok {
		// TODO: 写回和数据库时，要把old_key写回到leveldb
	}
	b.batch[string(key)] = value
	b.size += len(key) + len(value) - len(old_key)
	return nil
}

func (b *pmemBatch) Delete(key []byte) error {
	log.Error("(b *PmemBatch) Delete is not implemented")
	return nil
}

func (b *pmemBatch) ValueSize() int {
	return b.size
}

func write(pmemCache *PMemCache, batch_map map[string][]byte) {
	pmemBatchWriteStartMeter.Mark(1)
	start := time.Now()
	for k, v := range batch_map {
		pmemCache.Put([]byte(k), v)
	}
	pmemBatchWriteTimer.UpdateSince(start)
	pmemBatchWriteFinishMeter.Mark(1)
}

func (b *pmemBatch) Write() error {
	go write(b.pmem, b.batch)
	return nil
}

func (b *pmemBatch) Reset() {
	b.batch = make(map[string][]byte)
	b.size = 0
}

func PrintMetrics() {
	fmt.Println("Metrics in core/pmem_cache/cache.go:")
	fmt.Println("	core/pmem_cache/put_error.Count: ", pmemPutErrorMeter.Count())
	fmt.Println("	core/pmem_cache/put_error.Rate1: ", pmemPutErrorMeter.Rate1())
	fmt.Println("	core/pmem_cache/on_evict.Count: ", pmemOnEvictMeter.Count())
	fmt.Println("	core/pmem_cache/on_evict.Rate1: ", pmemOnEvictMeter.Rate1())
	fmt.Println("	core/pmem_cache/writeKV.Count: ", pmemWriteKVMeter.Count())
	fmt.Println("	core/pmem_cache/writeKV.Rate1: ", pmemWriteKVMeter.Rate1())
	fmt.Println("	core/pmem_cache/write_count.Count: ", pmemWriteCountMeter.Count())
	fmt.Println("	core/pmem_cache/write_count.Rate1: ", pmemWriteCountMeter.Rate1())
}
