#ifndef CONTAINER_POOLALLOCATOR_HH
#define CONTAINER_POOLALLOCATOR_HH

#include <string>

namespace container
{
   //========================================================================

   class PoolAllocator
   {
      public:

         virtual void * allocate () = 0;

         virtual void free (void * ptr) = 0;

         virtual void addMem (void * ptr, size_t size) = 0;

         virtual size_t getObjectSize () const = 0;

         virtual bool isFull () const = 0;

         virtual ~PoolAllocator ();
   };

   //========================================================================
}

#endif
