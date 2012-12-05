
#include "scrmfs-runtime-config.h"
#include "scrmfs-defs.h"

#ifdef HAVE_CONTAINER_LIB

#include <container.h>

int scrmfs_container_init(char * info, cs_store_handle_t * cs_store_handle){

   int ret = cs_store_init(info, cs_store_handle);
   return ret;

}

int scrmfs_container_finalize(){
   
}

int scrmfs_container_create(cs_store_handle_t * cs_store_handle, cs_set_handle_t * cs_set_handle){

   char prefix[100];
   int exclusive = 0;
   size_t size = SCRMFS_MAX_CHUNKS * SCRMFS_CHUNK_SIZE;
   sprintf(prefix,"cs_set1");

   int ret = cs_store_set_create (*cs_store_handle, prefix, size, exclusive, cs_set_handle);

   return ret;

}

#endif /* HAVE_CONTAINER_LIB */
