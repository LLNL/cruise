#ifndef CONTAINER_CONTAINERSTORE_HH
#define CONTAINER_CONTAINERSTORE_HH

#include "container-types.h"

#include "Container-fwd.hh"

#include "UniquePtr.hh"

namespace container
{
   //========================================================================

   class ContainerStore
   {
      public:
         ContainerStore (const char * info);

         ~ContainerStore ();


         int available (cs_off_t * available) const;

         ContainerSet * setOpen (const char * prefix);
         
         ContainerSet * setCreate (const char * prefix,
              size_t reserve, bool exclusive);

         int setRemove (const char * prefix);

      protected:

         friend class Container;
         friend class ContainerSet;

         StoreFormat & getFormat ();

         const StoreFormat & getFormat () const;

      protected:

         UniquePtr<BackingStore> store_;
         UniquePtr<StoreLock> lock_;
         UniquePtr<StoreFormat> format_;
   };

   //========================================================================
}

#endif
