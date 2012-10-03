#ifndef CONTAINER_SIMPLEPOOLALLOCATOR_HH
#define CONTAINER_SIMPLEPOOLALLOCATOR_HH

#include "PoolAllocator.hh"
#include "OffsetPtr.hh"


namespace container
{
   //========================================================================


   struct PoolBlockHeader
   {
      OffsetPtr<PoolBlockHeader> next_free;
   };

   struct PoolAllocatorData
   {
      uint32_t object_size;
      // freelist
      OffsetPtr<PoolBlockHeader> next_free;
   };

   /**
    * This class requires user to synchronize access to the allocation pool.
    */
   class SimplePoolAllocator : public PoolAllocator
   {
      public:
         SimplePoolAllocator (PoolAllocatorData * p, size_t objectsize);

         void addMem (void * ptr, size_t size);

         static SimplePoolAllocator * load (PoolAllocatorData * ptr);

         void * allocate ();

         void free (void * ptr);

         virtual ~SimplePoolAllocator ();

         bool isFull () const;

         size_t getObjectSize () const;

      protected:
         SimplePoolAllocator (PoolAllocatorData * p );

      protected:
         PoolAllocatorData & data_;
   };

   //========================================================================
}

#endif
