#ifndef CONTAINER_STORELOCK_HH
#define CONTAINER_STORELOCK_HH

#include <string.h> // size_t

namespace container
{
   //======================================================================== 


   struct StoreLock
   {

      /// Returns the number of times a write lock has been released
      virtual size_t lock (bool write) = 0;

      virtual size_t unlock (bool write) = 0;

      /// Return the number of times a write lock has been released.
      /// Note that this version is *shared* between all users of the lock.
      virtual size_t getVersion () const = 0;

      /// This function returns getWriteVersion if the lock is a read lock
      /// or not locked at all,
      /// and getWriteVersion + 1 if it is a write lock. 
      virtual size_t getAutoVersion () const = 0;

      virtual ~StoreLock ();

   };

   //======================================================================== 
} 
#endif
