
#include "scrmfs-runtime-config.h"
#include "scrmfs-internal.h"
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
        case CS_NOSTORAGE:
           ret = SCRMFS_ERR_NOSPC;
           break;
        case CS_EXISTS:
	   ret = SCRMFS_ERR_EXIST;
           break;
        case CS_INVALIDKEY:
        case CS_NOSUCHSET:
        case CS_NOSUCHCONTAINER:
        case CS_INVALID_FORMAT:
        case CS_NOTIMPLEMENTED:
           ret = SCRMFS_ERR_IO;
           break; 
        default:
           printf("UNKNOWN FAILURE CODE IN SCRMFS-CONTAINER.C\n");
           ret = SCRMFS_FAILURE; 
           break;
     }
     return ret;

}

int scrmfs_container_init(char * info, cs_store_handle_t * cs_store_handle, cs_set_handle_t * cs_set_handle, size_t size, const char * prefix){

   int ret = cs_store_init(info, cs_store_handle);
   if (ret != CS_SUCCESS){
      ret = translate_codes(ret);
      return ret;
   }
   int exclusive = 0;

   ret = cs_store_set_create (*cs_store_handle, prefix, size, exclusive, cs_set_handle);
   ret = translate_codes(ret);
   return ret;

}

int scrmfs_container_finalize(){
   
}


int scrmfs_container_open(cs_set_handle_t  cs_set_handle, cs_container_handle_t* ch, int fid, size_t size, const char * prefix){


   int create = 1;
   int created = 0;

   int ret = cs_set_container_open(cs_set_handle, prefix, size,
                      create, &created, ch);
   ret = translate_codes(ret);
   return ret;

}

int scrmfs_container_extend(cs_set_handle_t  cs_set_handle, cs_container_handle_t ch, off_t size){
   //this is not currently implemented in the container library, so always 
   //return no space
   return SCRMFS_ERR_NOSPC;
}

int scrmfs_container_read(cs_container_handle_t ch, void * buf, size_t count, off_t offset){

   size_t memcount = 1;
   size_t memsizes = count;
   size_t filecount = 1;
   cs_off_t fileofs = offset;
   cs_off_t filesizes = count;
   cs_off_t transferred = 0;


   int ret = cs_container_read (ch, memcount, &buf, &memsizes, filecount,
                  &fileofs, &filesizes, &transferred);
   ret = translate_codes(ret);
   return ret;

}

int scrmfs_container_write(cs_container_handle_t ch, void * buf, size_t count, off_t offset){
   size_t memsizes = count;
   cs_off_t fileofs = offset;
   cs_off_t filesizes = count;
   cs_off_t transferred;

   int ret = cs_container_write (ch, 1, &buf, &memsizes, 1,
                &fileofs, &filesizes, &transferred);
   ret = translate_codes(ret);
   return ret;

}

#endif /* HAVE_CONTAINER_LIB */
