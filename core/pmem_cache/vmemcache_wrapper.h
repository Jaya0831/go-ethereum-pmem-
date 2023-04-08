#ifndef LIBVMEMCACHE_WRAPPER_H
#define LIBVMEMCACHE_WRAPPER_H 1

#include <libvmemcache.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

VMEMcache* wrapper_vmemcache_new(const char *path, const size_t size, vmemcache_on_miss *miss);
char* wrapper_vmemcache_get(VMEMcache* cache, const void *key);
int wrapper_vmemcache_put(VMEMcache* cache, const void *key, const void *value);
int wrapper_vmemcache_exists(VMEMcache *cache, const void *key);
int wrapper_vmemcache_evict(VMEMcache *cache, const void *key);
void wrapper_vmemcache_delete(VMEMcache* cache);
#endif
