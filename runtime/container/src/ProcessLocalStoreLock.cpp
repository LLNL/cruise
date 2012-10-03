#include "ProcessLocalStoreLock.hh"

#include <assert.h>

namespace container
{
   //========================================================================

   size_t ProcessLocalStoreLock::lock (bool write)
   {
      if (write)
      {
         check (pthread_rwlock_wrlock (&rwlock_));
      }
      else
      {
         check (pthread_rwlock_rdlock (&rwlock_));
      }

      // This is safe, since we're holding at least a write or a read lock.
      // If we're holding the write lock, nobody else can modify
      // write_version_;
      // if we're holding a read lock, nobody else can modify write_version_
      // (since only the one holding the write lock can do so, and if we're
      // holding a read lock nobody can hold a write lock).
      return write_version_;
   }

   size_t ProcessLocalStoreLock::unlock (bool write)
   {
      // this is safe as only one thread can have the rwlock_ in write mode
      const size_t ret = (write ? ++write_version_ : write_version_ );
      check (pthread_rwlock_unlock (&rwlock_));
      return ret;
   }

   size_t ProcessLocalStoreLock::getWriteVersion () const
   {
      return write_version_;
   }

   ProcessLocalStoreLock::ProcessLocalStoreLock ()
      : write_version_ (0)
   {
      check (pthread_rwlock_init (&rwlock_, 0));
   }

   ProcessLocalStoreLock::~ProcessLocalStoreLock ()
   {
      check (pthread_rwlock_destroy (&rwlock_));
   }

   int ProcessLocalStoreLock::check (int ret) const
   {
      assert (ret == 0);
      return ret;
   }

   //========================================================================
}
