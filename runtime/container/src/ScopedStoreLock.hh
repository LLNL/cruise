#ifndef CONTAINER_SCOPEDBACKINGLOCK_HH
#define CONTAINER_SCOPEDBACKINGLOCK_HH


#include "Container-fwd.hh"

namespace container
{
   //========================================================================


   struct ScopedStoreLock
   {
      public:

         ScopedStoreLock (StoreLock & store, bool write);

         ~ScopedStoreLock ();

      protected:

         StoreLock & store_;
         bool write_;
   };


   //========================================================================
}

#endif
