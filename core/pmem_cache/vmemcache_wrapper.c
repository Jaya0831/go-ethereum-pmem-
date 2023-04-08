#include <libvmemcache.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "vmemcache_wrapper.h"

#define STR_AND_LEN(x) (x), strlen(x)

VMEMcache* wrapper_vmemcache_new(const char *path, const size_t size, vmemcache_on_miss *miss)
{
    VMEMcache* cache = vmemcache_new();
    vmemcache_set_size(cache, size);
    vmemcache_callback_on_miss(cache, miss, 0);
	if (vmemcache_add(cache, path)) {
		fprintf(stderr, "error: vmemcache_add: %s\n",
				vmemcache_errormsg());
        return NULL;
	}
    return cache;
}

char* wrapper_vmemcache_get(VMEMcache* cache, const void *key)
{
    // printf("---get_wrapper: key=%s, strlen(key)=%ld\n", (char *)key, strlen(key));
    //TODO: 这样会调用两次vmcache_index_get（exist和get各一次），可能需要改动vmemcache库 
	size_t vsize_c=0;
    int exist = vmemcache_exists(cache, STR_AND_LEN(key), &vsize_c);
    // printf("---vsize_c in wrapper_vmemcache_get=%ld\n", vsize_c);
    if (exist!=1)
    {
        // printf("---not exist!\n");
        return NULL;
    }
    char *buf;
    buf=(char*)malloc((vsize_c)*sizeof(char));
    ssize_t len = vmemcache_get(cache, STR_AND_LEN(key), buf, (vsize_c)*sizeof(char), 0, NULL);
	if (len >= 0) {
        // printf("---%.*s\n", (int)len, buf);
        return buf;
    }
	else {
        // printf("---(key not found: %s)\n", (char *)key);
        return NULL;
    }
}

// return 0 on success, -1 on error
// if the key is already exist, replace it.
int wrapper_vmemcache_put(VMEMcache* cache, const void *key, const void *value)
{
    // printf("---put_wrapper: ");
    // printf("key=%s, strlen(key)=%ld, ", (char *)key, strlen(key));
    // printf("value=%s, strlen(value)=%ld\n", (char *)value, strlen(value));
    vmemcache_evict(cache, STR_AND_LEN(key));
    //FIXME: 当cache不写穿时，如果crash在这里发生，怎么保持consistency
    return vmemcache_put(cache, STR_AND_LEN(key), STR_AND_LEN(value));
}


// This function does not impact the replacement policy or statistics.
// if the key exists, return the size of value
// if not found, return -1
// return -2 if the search couldn't be performed.
int wrapper_vmemcache_exists(VMEMcache *cache, const void *key)
{
    unsigned long vsize;
    int tmp = vmemcache_exists(cache, STR_AND_LEN(key), &vsize);
    if (tmp==1) return vsize;
    if (tmp==0) return -1;
    else return -2;
}

void wrapper_vmemcache_delete(VMEMcache* cache)
{
    vmemcache_delete(cache);
}

// Returns 0 if an entry has been evicted, -1 otherwise.
int wrapper_vmemcache_evict(VMEMcache *cache, const void *key)
{
    vmemcache_evict(cache, STR_AND_LEN(key));
}