#ifndef CONTAINER_H
#define CONTAINER_H

#include <stddef.h>  /* for size_t */

//#include "container_config.h"
#include "container-types.h"

#ifdef __cplusplus
namespace container
{
   extern "C"
   {
#endif


struct cs_store_impl_t;
typedef struct cs_store_impl_t * cs_store_handle_t;

struct cs_set_impl_t;
typedef struct cs_set_impl_t * cs_set_handle_t;

struct cs_container_impl_t;
typedef struct cs_container_impl_t * cs_container_handle_t;


/* ========================================================= */
/* ========= Container Store =============================== */
/* ========================================================= */

/**
 * info: meta-data used for debugging
 * handle: output: library handle
 *
 * Returns CS_SUCCESS, or CS_ERR otherwise
 */
int cs_store_init (const char * info, cs_store_handle_t * handle);


/**
 * Close library.
 * Returns CS_SUCCESS if OK
 */
int cs_store_finalize (cs_store_handle_t handle);

/**
 * Query free (i.e. not used or reserved) space in the container space.
 */
int cs_store_available (cs_store_handle_t handle, cs_off_t * available);

/**
 * Open existing container set.
 */
int cs_store_set_open (cs_store_handle_t chandle, const char * prefix,
      cs_set_handle_t * handle);

/*
 * Create new or open existing container set.
 * - exclusive: if true, operation will fail if set already exists.
 * - if reserve is non-zero, will reserve at least reserve bytes for the set.
 *   Will fail with C_NOSPACE if reservation cannot be guaranteed.
 */
int cs_store_set_create (cs_store_handle_t chandle, const char * prefix,
      size_t reserve, int exclusive, cs_set_handle_t * handle);


/**
 * Remove container set (including all container within).
 */
int cs_store_set_remove (cs_store_handle_t chandle, const char * prefix);

/* ========================================== */
/* ==== Container Set ======================= */
/* ========================================== */

/**
 * Return amount of free space in container set.
 * Returns minimum guaranteed free space in this set,
 * and current (non-guaranteed) space available in the set.
 */
int cs_set_available (cs_set_handle_t handle, cs_off_t * reserved_free,
      cs_off_t * max_free);

/**
 * Create a container with given name and size in the specified set.
 * If create != true, fails if container does not exist.
 * Otherwise, creates if needed and sets created accordingly
 */
int cs_set_container_open (cs_set_handle_t handle, const char * name,
      cs_off_t size, int create, int * created,
      cs_container_handle_t * newhandle);

int cs_set_container_remove (cs_set_handle_t handle, const char * name);

/**
 * List entries in the set.
 * maxentries: space in entries array
 * entries(output): number of entries in the set
 * min (entries, maxentries) container names are returned in the entries
 * array.
 */
int cs_set_list (cs_set_handle_t set,
      cs_container_t * entries, size_t maxentries, size_t * ret_entries);


int cs_set_close (cs_set_handle_t set);


/* ========================================== */
/* ========= Container ====================== */
/* ========================================== */

int cs_container_read (cs_container_handle_t handle, size_t
      memcount, void * membuf[], size_t memsizes[], size_t filecount,
      cs_off_t fileofs[], cs_off_t filesizes[], cs_off_t * transferred);

int cs_container_write (cs_container_handle_t container,
      size_t memcount,
      const void * membuf[], size_t memsizes[], size_t filecount,
      cs_off_t fileofs[], cs_off_t filesizes[], cs_off_t * transferred);

int cs_container_size (cs_container_handle_t handle,
      cs_off_t * size);


int cs_container_close (cs_container_handle_t handle);


#ifdef __cplusplus
      } /* extern C */
} /* namespace */
#endif

/**
 * NOTES (nothing to worry about until we run into problems)
 *   - might need sync to explicitly make changes visible to other
 *     processes
 *   - specifying strings all the time might be slow.
 *     In that case, have open/create return a container entry handle.
 *
 *   - write_in_place, read_in_place need to be added.
 *     NOTE: won't be able to guarantee in wrapper.
 *
 *   - Don't try to do integrity. Applications that want it can do it
 *     themselves.
 */

#endif

