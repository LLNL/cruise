#ifndef CONTAINER_TYPES_H
#define CONTAINER_TYPES_H

#include <stdint.h>
#include <stddef.h>  /* for size_t */

#ifdef __cplusplus
namespace container
{
   extern "C"
   {
#endif


typedef uint64_t cs_off_t;

enum
{
   CS_SUCCESS = 0,
   CS_NOSPACE,           /* No more space available */
   CS_NOSTORAGE,         /* No storage available on this node */
   CS_INVALIDKEY,        /* Key does not match set key */
   CS_NOSUCHSET,         /* Set does not exist */
   CS_NOSUCHCONTAINER,   /* Container does not exist */
   CS_INVALID_FORMAT,    /* No container storage or corrupted */
   CS_EXISTS,            /* Entity already existed */
   CS_NOTIMPLEMENTED     /* Functionality not available */
};

enum
{
   /* No information about useable lifetime can be given */
   C_LIFETIME_DEFAULT = 0,

   /** Set can be removed when the set is closed */
   C_LIFETIME_SESSION,

   /** Data can be removed after the job ends */
   C_LIFETIME_JOB
};

enum {
   /* number of usable characters in a container name */
   CS_NAME_CONTAINER_MAX = 127,

   /* Maximum number of characters in prefix */
   CS_NAME_PREFIX_MAX = 64
};


enum
{
   CS_LIST_NAME = 0x01,
   CS_LIST_SIZE = 0x02
};

typedef struct
{
   char name[CS_NAME_CONTAINER_MAX+1];
   cs_off_t size;
} cs_container_t;


#ifdef __cplusplus
    }
}
#endif

#endif
