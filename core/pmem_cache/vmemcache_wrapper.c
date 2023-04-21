#include <libvmemcache.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "vmemcache_wrapper.h"


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

get_ret wrapper_vmemcache_get(VMEMcache* cache, const void *key, size_t key_size)
{
    // printf("wrapper_vmemcache_get.key_size: %ld\n", key_size);
    // printf("wrapper_vmemcache_get.key: [");
    // for (int i = 0; i < key_size; i++)
    // {
    //     printf("%d ", ((char*)key)[i]);
    // }
    // printf("]\n");
    //TODO: 这样会调用两次vmcache_index_get（exist和get各一次），可能需要改动vmemcache库 
	size_t vsize_c=0;
    int exist = vmemcache_exists(cache, key, key_size, &vsize_c);
    // printf("wrapper_vmemcache_get.vsize_c: %ld\n", vsize_c);
    if (exist!=1)
    {
        // printf("---not exist!\n");
        get_ret ret={0, NULL};
        return ret;
    }
    void *buf;
    buf=malloc((vsize_c)*sizeof(char));
    ssize_t len = vmemcache_get(cache, key, key_size, buf, (vsize_c)*sizeof(char), 0, NULL);
	if (len >= 0) {
        // printf("wrapper_vmemcache_get.Hit\n");
        get_ret ret={(vsize_c)*sizeof(char), buf};
        return ret;
    }
	else {
        // printf("wrapper_vmemcache_get.Miss\n");
        get_ret ret={0, NULL};
        return ret;
    }
}

// return 0 on success, -1 on error
// if the key is already exist, replace it.
int wrapper_vmemcache_put(VMEMcache* cache, const void *key, size_t key_size, const void *value, size_t value_size)
{
    // printf("---put_wrapper: key_size=%ld, value_size=%ld\n", key_size, value_size);
    // printf("wrapper_vmemcache_put.key: [");
    // for (int i = 0; i < key_size; i++)
    // {
    //     printf("%d ", ((char*)key)[i]);
    // }
    // printf("], value: []");
    // for (int i = 0; i < value_size; i++)
    // {
    //     printf("%d ", ((char*)value)[i]);
    // }
    // printf("]\n");
    vmemcache_evict_no_trigger(cache, key, key_size);
    //FIXME: 当cache不写穿时，如果crash在这里发生，怎么保持consistency
    return vmemcache_put(cache, key, key_size, value, value_size);
}

// This function does not impact the replacement policy or statistics.
// if the key exists, return the size of value
// if not found, return -1
// return -2 if the search couldn't be performed.
int wrapper_vmemcache_exists(VMEMcache *cache, const void *key, size_t key_size)
{
    unsigned long vsize;
    int tmp = vmemcache_exists(cache, key, key_size, &vsize);
    if (tmp==1) return vsize;
    if (tmp==0) return -1;
    else return -2;
}

void wrapper_vmemcache_delete(VMEMcache* cache)
{
    vmemcache_delete(cache);
}

// Returns 0 if an entry has been evicted, -1 otherwise.
int wrapper_vmemcache_evict(VMEMcache *cache, const void *key, size_t key_size)
{
    vmemcache_evict(cache, key, key_size);
}