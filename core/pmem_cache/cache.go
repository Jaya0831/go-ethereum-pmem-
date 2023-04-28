package pmem_cache

//#cgo CFLAGS: -I.
//#cgo LDFLAGS: -L${SRCDIR} -lvmemcache_wrapper -lvmemcache
//#include <stdlib.h>
//#include "vmemcache_wrapper.h"
//extern void on_miss(VMEMcache*, void*, size_t, void*);
//extern void on_evict(VMEMcache*, void*, size_t, void*);
import "C"
import (
	"sync"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/syndtr/goleveldb/leveldb"
)

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
	pmemCacheCurrent *PmemCache = nil
	currentLock      sync.Mutex

	// metrics
	pmemNewMeter        = metrics.NewRegisteredMeter("core/pmem_cache/new", nil)
	pmemCloseMeter      = metrics.NewRegisteredMeter("core/pmem_cache/close", nil)
	pmemPutError        = metrics.NewRegisteredMeter("core/pmem_cache/put_error", nil)
	pmemPutInconsistent = metrics.NewRegisteredMeter("core/pmem_cache/put_inconsistent", nil)
	pmemOnEvict         = metrics.NewRegisteredMeter("core/pmem_cache/on_evict", nil)
	pmemOnEvictDirty    = metrics.NewRegisteredMeter("core/pmem_cache/on_evict_dirty", nil)
	pmemWriteKVMeter    = metrics.NewRegisteredMeter("core/pmem_cache/writeKV", nil)
	pmemDeleteMeter     = metrics.NewRegisteredMeter("core/pmem_cache/delete", nil)
	// pmemUpdateMeter     = metrics.NewRegisteredMeter("core/pmem_cache/update", nil)
	pmemWriteCountMeter = metrics.NewRegisteredMeter("core/pmem_cache/write_count", nil)

	pmemBatchWriteTimer       = metrics.NewRegisteredTimer("core/pmem_cache/batch_write", nil)
	pmemBatchWriteStartMeter  = metrics.NewRegisteredMeter("core/pmem_cache/batch_write_start", nil)
	pmemBatchWriteFinishMeter = metrics.NewRegisteredMeter("core/pmem_cache/batch_write_finish", nil)
)

type PmemCache struct {
	cache         *C.VMEMcache
	under_leveldb *leveldb.DB
	pmemWriteLock *sync.Mutex
	write_map     map[string][]byte //[hash, node.rlp]
}

func registerPmemCache(pmemCache *PmemCache) error {
	currentLock.Lock()
	defer currentLock.Unlock()
	if pmemCacheCurrent != nil {
		return NewPmemError("RegisterPmemCache Error")
	}
	pmemCacheCurrent = pmemCache
	return nil
}

func GetPmemCache() *PmemCache {
	return pmemCacheCurrent
}

func NewPmemcache(db *leveldb.DB) *PmemCache {
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
	cache_size := int64(1024 * 1024 * 256 * 1) //1GB
	cache := C.wrapper_vmemcache_new(path_c, C.ulong(cache_size), (*C.vmemcache_on_evict)(C.on_evict))
	if cache == nil {
		return nil
	}
	pmemCache := &PmemCache{
		cache:         cache,
		under_leveldb: db,
		pmemWriteLock: &sync.Mutex{},
		write_map:     make(map[string][]byte),
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

// export on_miss: In our implementation, as wrapper_vmemcache_get will first call vmemcache_exists to ensure the existence of the key,
// on_miss won't be called in the normal situation.
func on_miss(cache *C.VMEMcache, key unsafe.Pointer, k_size C.ulong, args unsafe.Pointer) {
	log.Error("on_miss")
}

var (
	test_writeMapCounter = metrics.NewRegisteredCounter("a/writeMap", nil)
)

//export on_evict
func on_evict(cache *C.VMEMcache, key unsafe.Pointer, k_size C.ulong, args unsafe.Pointer) {
	// TODO: for test, on_evict should be nil during runtime
	pmemOnEvict.Mark(1)
	value_struct := C.wrapper_vmemcache_get(cache, key, C.ulong(k_size))
	if value_struct.buf == nil {
		log.Error("core/pmem_cache.on_evict_dirty: value_struct.buf=nil")
	}
	defer C.free(value_struct.buf)
	value_go := C.GoBytes(value_struct.buf, value_struct.len)
	pmemCacheCurrent.write_map[string(value_go[:common.HashLength])] = value_go[common.HashLength:]
	test_writeMapCounter.Inc(1)
	go evictToDiskdb(value_go[:common.HashLength], value_go[common.HashLength:])
}

func evictToDiskdb(key, value []byte) {
	db := pmemCacheCurrent.under_leveldb
	db.Put(key, value, nil)
	delete(pmemCacheCurrent.write_map, string(key))
	test_writeMapCounter.Dec(1)
}

// TODO: error is always nil
func (pmemCache *PmemCache) Get(key []byte, hash common.Hash) ([]byte, error) {
	if value := get(pmemCache.cache, key); value != nil {
		return value, nil
	}
	return append(hash[:], pmemCache.write_map[string(hash[:])]...), nil
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

func (pmemCache *PmemCache) Put(key []byte, value []byte) error {
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
	if tmp != 0 {
		pmemPutError.Mark(1)
		// if put error occurs, write the kv pair to leveldb
		go pmemCache.under_leveldb.Put(value[:common.HashLength], value[common.HashLength:], nil)
		// fmt.Println("Pmem Put Error:")
		// fmt.Println("len(key): ", len(key), "cap(key): ", cap(key), "key: ", key)
		// fmt.Println("len(value): ", len(value), "cap(value): ", cap(value), "value: ", value)
		// return NewPmemError("Pmem Put Error")
	}
	return nil
}

// Returns 0 if an entry has been deleted, -1 otherwise.
func (pmemCache *PmemCache) Delete(key []byte) error {
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

const IdealBatchSize = 100 * 1024

type PmemBatch struct {
	batch       map[string][]byte
	level_batch *leveldb.Batch
	pmem        *PmemCache
	size        int //level_batch+batch
}

func (pmemCache *PmemCache) NewPmemBatch() *PmemBatch {
	return &PmemBatch{
		batch:       make(map[string][]byte),
		level_batch: new(leveldb.Batch),
		pmem:        pmemCache,
		size:        0,
	}
}

func (b *PmemBatch) Put(key, value []byte) error {
	old_value, ok := b.batch[string(key)]
	if ok {
		b.level_batch.Put(old_value[:common.HashLength], old_value[common.HashLength:])
		b.size -= len(key)
	}
	b.batch[string(key)] = value
	b.size += len(key) + len(value)
	return nil
}

func (b *PmemBatch) Delete(key []byte) error {
	log.Error("(b *PmemBatch) Delete is not implemented")
	return nil
}

func (b *PmemBatch) ValueSize() int {
	return b.size
}

func write(pmemCache *PmemCache, batch_map map[string][]byte) {
	pmemBatchWriteStartMeter.Mark(1)
	start := time.Now()
	for k, v := range batch_map {
		pmemCache.Put([]byte(k), v)
	}
	pmemBatchWriteTimer.UpdateSince(start)
	pmemBatchWriteFinishMeter.Mark(1)
}

var (
	test_leveldbBatchWriteTimer = metrics.NewRegisteredTimer("a/leveldbBatchWrite", nil)
)

func (b *PmemBatch) Write() error {
	go write(b.pmem, b.batch)
	start := time.Now()
	b.pmem.under_leveldb.Write(b.level_batch, nil)
	test_leveldbBatchWriteTimer.UpdateSince(start)
	return nil
}

func (b *PmemBatch) Reset() {
	b.batch = make(map[string][]byte)
	b.level_batch.Reset()
	b.size = 0
}
