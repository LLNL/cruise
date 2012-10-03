#ifndef CONTAINER_SIMPLEALLOCATOR_HH
#define CONTAINER_SIMPLEALLOCATOR_HH

#include "Allocator.hh"
#include "OffsetPtr.hh"
#include "OffsetList.hh"

namespace container
{
   //========================================================================

   /**
    * Probably need to make this into a page allocator;
    * Then, for the fixed size items, use a pool allocator fed by pages from
    * this allocator.
    */
   class SimpleAllocator : public Allocator
   {
      public:
         SimpleAllocator (void * ptr, size_t size,
               bool format = false);

         virtual ~SimpleAllocator ();

      public:
         virtual void * allocate (size_t size, size_t minsize, size_t * out);

         virtual void free (void * ptr, size_t size);

         virtual size_t available () const;

      protected:
         struct BlockHeader;

         void splitBlock (BlockHeader * b, size_t size);

      protected:

         struct BlockHeader : public OffsetLink<BlockHeader>
         {
            uint64_t               size;  // Total Size of block
         };

         typedef OffsetList<BlockHeader> FreeListType;

         struct AllocHeader
         {
            uint64_t                  totalsize;  // Size of total region
            uint64_t                  available;  // Free space in allocator
            FreeListType              free;       // start of free list
         };

         AllocHeader * header_;
    };

   //========================================================================
}

#endif
