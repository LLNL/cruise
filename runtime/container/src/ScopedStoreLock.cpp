#include "ScopedStoreLock.hh"
#include "StoreLock.hh"

namespace container
{
   //========================================================================

   ScopedStoreLock::ScopedStoreLock (StoreLock & store, bool write)
      : store_(store), write_ (write)
   {
      store_.lock (write_);
   }

   ScopedStoreLock::~ScopedStoreLock ()
   {
      store_.unlock (write_);
   }

   //========================================================================
}
