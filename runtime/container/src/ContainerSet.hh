#ifndef CONTAINER_CONTAINERSET_HH
#define CONTAINER_CONTAINERSET_HH

#include "container-types.h"
#include "Container-fwd.hh"
#include "StoreFormat.hh"
#include "IntrusivePtrHelper.hh"

#include <string>

namespace container
{
   //========================================================================

   class ContainerSet : public IntrusivePtrHelper
   {
      protected:

         friend class ContainerStore;

         ContainerSet (ContainerStore & parent,
               const char * prefix);
               
         ContainerSet (ContainerStore & parent,
               const char * prefix, size_t reserve,
               bool exclusive);

      public:
         int available (cs_off_t * reserved, cs_off_t * max) const;

         Container *  containerOpen (const char * name, cs_off_t size,
               int create, int * created);

         int containerSize (const char * name, cs_off_t * size);

         int containerRemove (const char * name);

         int list (cs_container_t * entries, size_t max, size_t * actual);

         StoreFormat::SetHandle getHandle () const
         { return handle_; }


      protected:

         friend class Container;

         StoreFormat & getFormat ();

         const StoreFormat & getFormat () const;

      protected:
         StoreFormat::SetHandle handle_;
         std::string prefix_;

         ContainerStore & parent_;
   };

   INTRUSIVE_PTR_HELPER (ContainerSet);

   //========================================================================
}

#endif

