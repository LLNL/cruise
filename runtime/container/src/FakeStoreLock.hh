#ifndef CONTAINER_FAKESTORELOCK_HH
#define CONTAINER_FAKESTORELOCK_HH

#include "StoreLock.hh"

namespace container
{
   //========================================================================

   class FakeStoreLock : public StoreLock
   {
      public:
         FakeStoreLock ();

         size_t lock (bool write);

         size_t unlock (bool write);

         size_t getVersion () const;

         size_t getAutoVersion () const;


         virtual ~FakeStoreLock ();

      protected:
         size_t write_version_;
         bool   write_;
   };

   //========================================================================
}

#endif
