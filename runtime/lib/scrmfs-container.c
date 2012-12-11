
#include "scrmfs-runtime-config.h"
#include "scrmfs-defs.h"
#include "scrmfs-file.h"
#include <stdio.h>

#ifdef HAVE_CONTAINER_LIB

#include <container.h>

//TODO: translate CS error codes to SCRMFS codes
static int translate_codes(int CS_CODE){
     int ret = 0;
     switch (CS_CODE){
        case CS_SUCCESS:
           ret = SCRMFS_SUCCESS;
           break;
        case CS_NOSPACE:
           ret = SCRMFS_ERR_NOSPC;
           break;
        default:
           printf("UNKNOWN FAILURE CODE IN SCRMFS-CONTAINER.C\n");
           ret = SCRMFS_FAILURE; 
     }
     return ret;

}

int scrmfs_container_init(char * info, cs_store_handle_t * cs_store_handle, cs_set_handle_t * cs_set_handle, size_t size){

   int ret = cs_store_init(info, cs_store_handle);
   if (ret != CS_SUCCESS)
      ret = translate_codes(ret);
      return ret;
   char prefix[100];
   int exclusive = 0;
   //size_t size = SCRMFS_MAX_CHUNKS * SCRMFS_CHUNK_SIZE;
   //size_t size = SCRMFS_CHUNK_SIZE;
   sprintf(prefix,"cs_set1");

   ret = cs_store_set_create (*cs_store_handle, prefix, size, exclusive, cs_set_handle);
   ret = translate_codes(ret);
   return ret;

}

int scrmfs_container_finalize(){
   
}

//int scrmfs_container_create(cs_store_handle_t * cs_store_handle, cs_set_handle_t * cs_set_handle){


//}

int scrmfs_container_open(cs_set_handle_t * cs_set_handle, cs_container_handle_t** ch, int fid, size_t size, const char * prefix){

   //char prefix[100];
   //sprintf(prefix,"fid_%d", fid );

   int create = 1;
   int created = 0;
   //size_t size = 1 << SCRMFS_CHUNK_BITS;

   int ret = cs_set_container_open(*cs_set_handle, prefix, size,
                      create, &created, *ch);
   ret = translate_codes(ret);
   return ret;

}

int scrmfs_container_extend(cs_set_handle_t  cs_set_handle, cs_container_handle_t ch){
   //this is not currently implemented in the container library, so always 
   //return no space
   return SCRMFS_ERR_NOSPC;
}

#endif /* HAVE_CONTAINER_LIB */
