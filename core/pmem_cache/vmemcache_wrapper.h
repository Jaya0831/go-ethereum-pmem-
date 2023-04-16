#ifndef LIBVMEMCACHE_WRAPPER_H
#define LIBVMEMCACHE_WRAPPER_H 1

#include <libvmemcache.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct get_ret
{
    int len;
    void *buf;
}get_ret;

VMEMcache* wrapper_vmemcache_new(const char *path, const size_t size, vmemcache_on_evict *evict, vmemcache_on_evict_dirty *dirty);
get_ret wrapper_vmemcache_get(VMEMcache* cache, const void *key, size_t key_size);
int wrapper_vmemcache_put(VMEMcache* cache, const void *key, size_t key_size, const void *value, size_t value_size, char dirty);
int wrapper_vmemcache_exists(VMEMcache *cache, const void *key, size_t key_size);
int wrapper_vmemcache_evict(VMEMcache *cache, const void *key, size_t key_size);
void wrapper_vmemcache_delete(VMEMcache* cache);
#endif
