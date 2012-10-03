#include "container.h"
#include "ContainerStore.hh"
#include "ContainerSet.hh"
#include "Container.hh"
#include "ContainerException.hh"

namespace container
{
   extern "C"
   {
//===========================================================================


#define MAKE_STORE(a)      reinterpret_cast<ContainerStore *>(a)
#define MAKE_SET(a)        reinterpret_cast<ContainerSet *>(a)
#define MAKE_CONTAINER(a)  reinterpret_cast<Container *>(a)

int cs_store_init (const char * info, cs_store_handle_t * handle)
{
   try
   {
      *handle = (cs_store_handle_t) new ContainerStore (info);
      return CS_SUCCESS;
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}


int cs_store_finalize (cs_store_handle_t handle)
{
   try
   {
      /* might need to check here calling destructor */
      delete (MAKE_STORE(handle));
      return CS_SUCCESS;
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}

int cs_store_available (cs_store_handle_t handle, cs_off_t * available)
{
   try
   {
      return MAKE_STORE(handle)->available (available);
   }
   catch (ContainerException & e)
   {
      return e.getError ();
   }
}

int cs_store_set_open (cs_store_handle_t chandle, const char * prefix,
      cs_set_handle_t * handle)
{
   try
   {
      *handle = (cs_set_handle_t) MAKE_STORE(chandle)->setOpen (prefix);
      return (*handle ? CS_SUCCESS : CS_NOSUCHSET);
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}
      

int cs_store_set_create (cs_store_handle_t chandle, const char * prefix,
      size_t reserve, int exclusive, cs_set_handle_t * handle)
{
   try
   {
      *handle = (cs_set_handle_t) MAKE_STORE(chandle)->setCreate (prefix,
            reserve, exclusive);
      assert (*handle);
      return CS_SUCCESS;
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}


int cs_store_set_remove (cs_store_handle_t chandle, const char * prefix)
{
   try
   {
      return MAKE_STORE(chandle)->setRemove (prefix);
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}

/* ========================================== */
/* ==== Container Set ======================= */
/* ========================================== */

int cs_set_available (cs_set_handle_t handle, cs_off_t * reserved_free,
      cs_off_t * max_free)
{
   try
   {
      return MAKE_SET(handle)->available (reserved_free, max_free);
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}

int cs_set_container_open (cs_set_handle_t handle, const char * name,
      cs_off_t size, int create, int * created, 
      cs_container_handle_t * newhandle)
{
   try
   {
      *newhandle = (cs_container_handle_t) MAKE_SET(handle)->containerOpen (name,
            size, create, created);
      return CS_SUCCESS;
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}

int cs_set_container_remove (cs_set_handle_t handle, const char * name)
{
   try
   {
      return MAKE_SET(handle)->containerRemove (name);
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}

/**
 * List entries in the set.
 * maxentries: space in entries array
 * entries(output): number of entries in the set
 * min (entries, maxentries) container names are returned in the entries
 * array.
 */
int cs_set_list (cs_set_handle_t set,
      cs_container_t * entries, size_t maxentries, size_t * ret_entries)
{
   try
   {
   return MAKE_SET(set)->list (entries, maxentries, ret_entries);
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}


int cs_set_close (cs_set_handle_t set)
{
   try
   {
      delete (MAKE_SET(set));
      return CS_SUCCESS;
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}

/* ========================================== */
/* ========= Container ====================== */
/* ========================================== */

int cs_container_read (cs_container_handle_t handle, size_t
      memcount, void * membuf[], size_t memsizes[], size_t filecount,
      cs_off_t fileofs[], cs_off_t filesizes[], cs_off_t * transferred)
{
   try
   {
      return MAKE_CONTAINER(handle)->read (memcount, membuf, memsizes,
            filecount, fileofs, filesizes, transferred);
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}

int cs_container_write (cs_container_handle_t container,
      size_t memcount,
      const void * membuf[], size_t memsizes[], size_t filecount,
      cs_off_t fileofs[], cs_off_t filesizes[], cs_off_t * transferred)
{
   try
   {
      return MAKE_CONTAINER(container)->write (memcount, membuf, memsizes,
            filecount, fileofs, filesizes, transferred);
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}

int cs_container_close (cs_container_handle_t container)
{
   try
   {
      delete (MAKE_CONTAINER(container));
      return CS_SUCCESS;
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}

int cs_container_size (cs_container_handle_t handle,
      cs_off_t * size)
{
   try
   {
      return MAKE_CONTAINER(handle)->size (size);
   }
   catch (const ContainerException & e)
   {
      return e.getError ();
   }
}



//===========================================================================
     }
}
