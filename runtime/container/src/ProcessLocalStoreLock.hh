#ifndef CONTAINER_PROCESSLOCALSTORELOCK_HH
#define CONTAINER_PROCESSLOCALSTORELOCK_HH

#include "StoreLock.hh"

#include <pthread.h>

namespace container
{
   //========================================================================

   class ProcessLocalStoreLock : public StoreLock
   {
      public:
         virtual size_t lock (bool write);

         virtual size_t unlock (bool write);

         virtual size_t getWriteVersion () const;

         ProcessLocalStoreLock ();

         virtual ~ProcessLocalStoreLock ();

      protected:
         int check (int ret) const;

      protected:
         pthread_rwlock_t rwlock_;
         size_t           write_version_;
   };

   //========================================================================
}
#endif
