#ifndef CONTAINER_CONTAINER_HH
#define CONTAINER_CONTAINER_HH

#include "container-types.h"
#include "StoreFormat.hh"

#include <string>

namespace container
{
   //========================================================================

   class Container
   {
      public:
         Container (ContainerSet & parent, const char * name,
               cs_off_t size, int create, int * created);

         int read (size_t memcount, void * buf[], size_t sizes[],
               size_t filecount, cs_off_t fileofs[], cs_off_t filesizes[],
               cs_off_t * transferred);

         int write (size_t memcount, const void * buf[], size_t sizes[],
               size_t filecount, cs_off_t fileofs[], cs_off_t filesizes[],
               cs_off_t * transferred);


         int size (cs_off_t * size) const;

      protected:
         StoreFormat & getFormat ();

         const StoreFormat & getFormat () const;

         StoreFormat::ContainerHandle getHandle ()
         { return handle_; }
         
         const StoreFormat::ContainerHandle getHandle () const
         { return handle_; }

      protected:

         ContainerSet & parent_;
         std::string name_;
         StoreFormat::ContainerHandle handle_;
   };

   //========================================================================
}

#endif
