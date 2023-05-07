// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package rawdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"golang.org/x/crypto/sha3"
)

// HashScheme is the legacy hash-based state scheme with which trie nodes are
// stored in the disk with node hash as the database key. The advantage of this
// scheme is that different versions of trie nodes can be stored in disk, which
// is very beneficial for constructing archive nodes. The drawback is it will
// store different trie nodes on the same path to different locations on the disk
// with no data locality, and it's unfriendly for designing state pruning.
//
// Now this scheme is still kept for backward compatibility, and it will be used
// for archive node and some other tries(e.g. light trie).
const HashScheme = "hashScheme"

// PathScheme is the new path-based state scheme with which trie nodes are stored
// in the disk with node path as the database key. This scheme will only store one
// version of state data in the disk, which means that the state pruning operation
// is native. At the same time, this scheme will put adjacent trie nodes in the same
// area of the disk with good data locality property. But this scheme needs to rely
// on extra state diffs to survive deep reorg.
const PathScheme = "pathScheme"

// nodeHasher used to derive the hash of trie node.
type nodeHasher struct{ sha crypto.KeccakState }

var hasherPool = sync.Pool{
	New: func() interface{} { return &nodeHasher{sha: sha3.NewLegacyKeccak256().(crypto.KeccakState)} },
}

func newNodeHasher() *nodeHasher       { return hasherPool.Get().(*nodeHasher) }
func returnHasherToPool(h *nodeHasher) { hasherPool.Put(h) }

func (h *nodeHasher) hashData(data []byte) (n common.Hash) {
	h.sha.Reset()
	h.sha.Write(data)
	h.sha.Read(n[:])
	return n
}

// ReadAccountTrieNode retrieves the account trie node and the associated node
// hash with the specified node path.
func ReadAccountTrieNode(db ethdb.KeyValueReader, path []byte) ([]byte, common.Hash) {
	data, err := db.Get(accountTrieNodeKey(path))
	if err != nil {
		return nil, common.Hash{}
	}
	hasher := newNodeHasher()
	defer returnHasherToPool(hasher)
	return data, hasher.hashData(data)
}

// HasAccountTrieNode checks the account trie node presence with the specified
// node path and the associated node hash.
func HasAccountTrieNode(db ethdb.KeyValueReader, path []byte, hash common.Hash) bool {
	data, err := db.Get(accountTrieNodeKey(path))
	if err != nil {
		return false
	}
	hasher := newNodeHasher()
	defer returnHasherToPool(hasher)
	return hasher.hashData(data) == hash
}

// WriteAccountTrieNode writes the provided account trie node into database.
func WriteAccountTrieNode(db ethdb.KeyValueWriter, path []byte, node []byte) {
	if err := db.Put(accountTrieNodeKey(path), node); err != nil {
		log.Crit("Failed to store account trie node", "err", err)
	}
}

// DeleteAccountTrieNode deletes the specified account trie node from the database.
func DeleteAccountTrieNode(db ethdb.KeyValueWriter, path []byte) {
	if err := db.Delete(accountTrieNodeKey(path)); err != nil {
		log.Crit("Failed to delete account trie node", "err", err)
	}
}

// ReadStorageTrieNode retrieves the storage trie node and the associated node
// hash with the specified node path.
func ReadStorageTrieNode(db ethdb.KeyValueReader, accountHash common.Hash, path []byte) ([]byte, common.Hash) {
	data, err := db.Get(storageTrieNodeKey(accountHash, path))
	if err != nil {
		return nil, common.Hash{}
	}
	hasher := newNodeHasher()
	defer returnHasherToPool(hasher)
	return data, hasher.hashData(data)
}

// HasStorageTrieNode checks the storage trie node presence with the provided
// node path and the associated node hash.
func HasStorageTrieNode(db ethdb.KeyValueReader, accountHash common.Hash, path []byte, hash common.Hash) bool {
	data, err := db.Get(storageTrieNodeKey(accountHash, path))
	if err != nil {
		return false
	}
	hasher := newNodeHasher()
	defer returnHasherToPool(hasher)
	return hasher.hashData(data) == hash
}

// WriteStorageTrieNode writes the provided storage trie node into database.
func WriteStorageTrieNode(db ethdb.KeyValueWriter, accountHash common.Hash, path []byte, node []byte) {
	if err := db.Put(storageTrieNodeKey(accountHash, path), node); err != nil {
		log.Crit("Failed to store storage trie node", "err", err)
	}
}

// DeleteStorageTrieNode deletes the specified storage trie node from the database.
func DeleteStorageTrieNode(db ethdb.KeyValueWriter, accountHash common.Hash, path []byte) {
	if err := db.Delete(storageTrieNodeKey(accountHash, path)); err != nil {
		log.Crit("Failed to delete storage trie node", "err", err)
	}
}

var (
	// metrics
	pmemHitMeter    = metrics.NewRegisteredMeter("core/rawdb/accessors_trie/pmem/hit", nil)
	pmemMissMeter   = metrics.NewRegisteredMeter("core/rawdb/accessors_trie/pmem/miss", nil)
	pmemReadMeter   = metrics.NewRegisteredMeter("core/rawdb/accessors_trie/pmem/read", nil)
	pmemGetTimer    = metrics.NewRegisteredTimer("core/rawdb/accessors_trie/pmem/get_time", nil)
	levelDBGetTimer = metrics.NewRegisteredTimer("core/rawdb/accessors_trie/levelDB/get_time", nil)
	getTimer        = metrics.NewRegisteredTimer("core/rawdb/accessors_trie/get_time", nil)
	getMeter        = metrics.NewRegisteredMeter("core/rawdb/accessors_trie/get", nil)

	// metrics in trie/database.go
	MemcacheCleanHitMeter   = metrics.NewRegisteredMeter("trie/memcache/clean/hit", nil)
	MemcacheCleanMissMeter  = metrics.NewRegisteredMeter("trie/memcache/clean/miss", nil)
	MemcacheCleanReadMeter  = metrics.NewRegisteredMeter("trie/memcache/clean/read", nil)
	MemcacheCleanWriteMeter = metrics.NewRegisteredMeter("trie/memcache/clean/write", nil)

	MemcacheDirtyHitMeter   = metrics.NewRegisteredMeter("trie/memcache/dirty/hit", nil)
	MemcacheDirtyMissMeter  = metrics.NewRegisteredMeter("trie/memcache/dirty/miss", nil)
	MemcacheDirtyReadMeter  = metrics.NewRegisteredMeter("trie/memcache/dirty/read", nil)
	MemcacheDirtyWriteMeter = metrics.NewRegisteredMeter("trie/memcache/dirty/write", nil)
)
var (
	pre_getTime         float64 = 0
	pre_getCount        int64   = 0
	pre_leveldbGetTime  float64 = 0
	pre_leveldbGetCount int64   = 0
	pre_pmemGetTime     float64 = 0
	pre_pmemGetCount    int64   = 0
)

func PrintMetric() {
	fmt.Println("Metrics in core/rawdb/accessors_trie.go:")
	fmt.Println("	core/rawdb/accessors_trie/pmem/hit.Count: ", pmemHitMeter.Count())
	fmt.Println("	core/rawdb/accessors_trie/pmem/hit.Rate1: ", pmemHitMeter.Rate1())
	fmt.Println("	core/rawdb/accessors_trie/pmem/miss.Count: ", pmemMissMeter.Count())
	fmt.Println("	core/rawdb/accessors_trie/pmem/miss.Rate1: ", pmemMissMeter.Rate1())
	fmt.Println("	core/rawdb/accessors_trie/pmem/read.Count: ", pmemReadMeter.Count())
	fmt.Println("	core/rawdb/accessors_trie/pmem/read.Rate1: ", pmemReadMeter.Rate1())
	fmt.Println("	core/rawdb/accessors_trie/pmem/get_time.Mean: ", pmemGetTimer.Mean())
	fmt.Println("	core/rawdb/accessors_trie/pmem/get_time.Count: ", pmemGetTimer.Count())
	tmp := (pmemGetTimer.Mean()*float64(pmemGetTimer.Count()) - pre_pmemGetTime*float64(pre_pmemGetCount)) / (float64(pmemGetTimer.Count()) - float64(pre_pmemGetCount))
	fmt.Println("	core/rawdb/accessors_trie/pmem/get_time.Recent: ", tmp)
	fmt.Println("	core/rawdb/accessors_trie/levelDB/get_time.Mean: ", levelDBGetTimer.Mean())
	fmt.Println("	core/rawdb/accessors_trie/levelDB/get_time.Count: ", levelDBGetTimer.Count())
	tmp = (levelDBGetTimer.Mean()*float64(levelDBGetTimer.Count()) - pre_leveldbGetTime*float64(pre_leveldbGetCount)) / (float64(levelDBGetTimer.Count()) - float64(pre_leveldbGetCount))
	fmt.Println("	core/rawdb/accessors_trie/levelDB/get_time.Recent: ", tmp)
	fmt.Println("	core/rawdb/accessors_trie/get_time.Mean: ", getTimer.Mean())
	fmt.Println("	core/rawdb/accessors_trie/get_time.Count: ", getTimer.Count())
	tmp = (getTimer.Mean()*float64(getTimer.Count()) - pre_getTime*float64(pre_getCount)) / (float64(getTimer.Count()) - float64(pre_getCount))
	fmt.Println("	core/rawdb/accessors_trie/get_time.Recent: ", tmp)
	fmt.Println("	core/rawdb/accessors_trie/get.Count: ", getMeter.Count())
	fmt.Println("	core/rawdb/accessors_trie/get.Rate1: ", getMeter.Rate1())
	fmt.Println("Metrics in trie/database.go:")
	fmt.Println("	trie/memcache/clean/hit.Count: ", MemcacheCleanHitMeter.Count())
	fmt.Println("	trie/memcache/clean/hit.Rate1: ", MemcacheCleanHitMeter.Rate1())
	fmt.Println("	trie/memcache/clean/miss.Count: ", MemcacheCleanMissMeter.Count())
	fmt.Println("	trie/memcache/clean/miss.Rate1: ", MemcacheCleanMissMeter.Rate1())
	fmt.Println("	trie/memcache/clean/read.Count: ", MemcacheCleanReadMeter.Count())
	fmt.Println("	trie/memcache/clean/read.Rate1: ", MemcacheCleanReadMeter.Rate1())
	fmt.Println("	trie/memcache/clean/write.Count: ", MemcacheCleanWriteMeter.Count())
	fmt.Println("	trie/memcache/clean/write.Rate1: ", MemcacheCleanWriteMeter.Rate1())
	fmt.Println("	trie/memcache/dirty/hit.Count: ", MemcacheDirtyHitMeter.Count())
	fmt.Println("	trie/memcache/dirty/hit.Rate1: ", MemcacheDirtyHitMeter.Rate1())
	fmt.Println("	trie/memcache/dirty/miss.Count: ", MemcacheDirtyMissMeter.Count())
	fmt.Println("	trie/memcache/dirty/miss.Rate1: ", MemcacheDirtyMissMeter.Rate1())
	fmt.Println("	trie/memcache/dirty/read.Count: ", MemcacheDirtyReadMeter.Count())
	fmt.Println("	trie/memcache/dirty/read.Rate1: ", MemcacheDirtyReadMeter.Rate1())
	fmt.Println("	trie/memcache/dirty/write.Count: ", MemcacheDirtyWriteMeter.Count())
	fmt.Println("	trie/memcache/dirty/write.Rate1: ", MemcacheDirtyWriteMeter.Rate1())

	pre_getCount = getTimer.Count()
	pre_getTime = getTimer.Mean()
	pre_leveldbGetCount = levelDBGetTimer.Count()
	pre_leveldbGetTime = levelDBGetTimer.Mean()
	pre_pmemGetCount = pmemGetTimer.Count()
	pre_pmemGetTime = pmemGetTimer.Mean()
}

// ReadLegacyTrieNode retrieves the legacy trie node with the given
// associated node hash.
func ReadLegacyTrieNode(db ethdb.KeyValueReader, hash common.Hash) []byte {
	// TODO: what about the ReadlegacyTrieNode call in cmd/geth/snapshot.go
	getMeter.Mark(1)
	start := time.Now()
	pmdb, ok := db.(ethdb.Database)
	if ok {
		start_pmem := time.Now()
		enc_p, _ := pmdb.Pmem_Get(hash[:])
		pmemGetTimer.UpdateSince(start_pmem)
		if enc_p != nil {
			pmemReadMeter.Mark(int64(len(enc_p)))
			pmemHitMeter.Mark(1)
			getTimer.UpdateSince(start)
			return enc_p
		}
	}
	start_leveldb := time.Now()
	data, err := db.Get(hash.Bytes())
	levelDBGetTimer.UpdateSince(start_leveldb)
	if err != nil || data == nil {
		getTimer.UpdateSince(start)
		return nil
	}
	if ok {
		pmdb.Pmem_Put(hash[:], data)
		pmemMissMeter.Mark(1)
	}
	getTimer.UpdateSince(start)
	return data
}

// HasLegacyTrieNode checks if the trie node with the provided hash is present in db.
func HasLegacyTrieNode(db ethdb.KeyValueReader, hash common.Hash) bool {
	if pmdb, ok2 := db.(ethdb.Database); ok2 {
		if has, _ := pmdb.Pmem_Has(hash.Bytes()); has {
			return has
		}
	}
	ok, _ := db.Has(hash.Bytes())
	return ok
}

var (
	levelBatchMeter = metrics.NewRegisteredMeter("core/rawdb/accessors_trie/levelBatch", nil)
	pmemBatchMeter  = metrics.NewRegisteredMeter("core/rawdb/accessors_trie/pmemBatch", nil)
)

// WriteLegacyTrieNode writes the provided legacy trie node to database.
func WriteLegacyTrieNode(db ethdb.KeyValueWriter, hash common.Hash, node []byte) {
	if err := db.Put(hash.Bytes(), node); err != nil {
		log.Crit("Failed to store legacy trie node", "err", err)
	}
	if pmdb, ok := db.(ethdb.Database); ok {
		pmdb.Pmem_Put(hash.Bytes(), node)
	}
	// if b, ok2 := db.(*leveldb.LeveldbBatch); ok2 {
	// 	b.IsTrieData = true
	// }

}

// DeleteLegacyTrieNode deletes the specified legacy trie node from database.
func DeleteLegacyTrieNode(db ethdb.KeyValueWriter, hash common.Hash) {
	// println("DeleteLegacyTrieNode")
	//TODO: pmem cache
	if err := db.Delete(hash.Bytes()); err != nil {
		log.Crit("Failed to delete legacy trie node", "err", err)
	}
	if pmdb, ok := db.(ethdb.Database); ok {
		pmdb.Pmem_Delete(hash.Bytes())
	}
	// if pmdb, ok := db.(ethdb.Database); ok {
	// 	pmdb.Pmem_Delete(hash.Bytes())
	// } else if b, ok2 := db.(*leveldb.LeveldbBatch); ok2 {
	// 	b.IsTrieData = true
	// }
}

// HasTrieNode checks the trie node presence with the provided node info and
// the associated node hash.
func HasTrieNode(db ethdb.KeyValueReader, owner common.Hash, path []byte, hash common.Hash, scheme string) bool {
	switch scheme {
	case HashScheme:
		return HasLegacyTrieNode(db, hash)
	case PathScheme:
		if owner == (common.Hash{}) {
			return HasAccountTrieNode(db, path, hash)
		}
		return HasStorageTrieNode(db, owner, path, hash)
	default:
		panic(fmt.Sprintf("Unknown scheme %v", scheme))
	}
}

// ReadTrieNode retrieves the trie node from database with the provided node info
// and associated node hash.
// hashScheme-based lookup requires the following:
//   - hash
//
// pathScheme-based lookup requires the following:
//   - owner
//   - path
func ReadTrieNode(db ethdb.KeyValueReader, owner common.Hash, path []byte, hash common.Hash, scheme string) []byte {
	switch scheme {
	case HashScheme:
		return ReadLegacyTrieNode(db, hash)
	case PathScheme:
		var (
			blob  []byte
			nHash common.Hash
		)
		if owner == (common.Hash{}) {
			blob, nHash = ReadAccountTrieNode(db, path)
		} else {
			blob, nHash = ReadStorageTrieNode(db, owner, path)
		}
		if nHash != hash {
			return nil
		}
		return blob
	default:
		panic(fmt.Sprintf("Unknown scheme %v", scheme))
	}
}

// WriteTrieNode writes the trie node into database with the provided node info
// and associated node hash.
// hashScheme-based lookup requires the following:
//   - hash
//
// pathScheme-based lookup requires the following:
//   - owner
//   - path
func WriteTrieNode(db ethdb.KeyValueWriter, owner common.Hash, path []byte, hash common.Hash, node []byte, scheme string) {
	switch scheme {
	case HashScheme:
		WriteLegacyTrieNode(db, hash, node)
	case PathScheme:
		if owner == (common.Hash{}) {
			WriteAccountTrieNode(db, path, node)
		} else {
			WriteStorageTrieNode(db, owner, path, node)
		}
	default:
		panic(fmt.Sprintf("Unknown scheme %v", scheme))
	}
}

// DeleteTrieNode deletes the trie node from database with the provided node info
// and associated node hash.
// hashScheme-based lookup requires the following:
//   - hash
//
// pathScheme-based lookup requires the following:
//   - owner
//   - path
func DeleteTrieNode(db ethdb.KeyValueWriter, owner common.Hash, path []byte, hash common.Hash, scheme string) {
	switch scheme {
	case HashScheme:
		DeleteLegacyTrieNode(db, hash)
	case PathScheme:
		if owner == (common.Hash{}) {
			DeleteAccountTrieNode(db, path)
		} else {
			DeleteStorageTrieNode(db, owner, path)
		}
	default:
		panic(fmt.Sprintf("Unknown scheme %v", scheme))
	}
}
