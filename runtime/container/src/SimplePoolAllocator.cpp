#include "SimplePoolAllocator.hh"
#include <algorithm>

namespace container
{
   //========================================================================

   SimplePoolAllocator::SimplePoolAllocator (PoolAllocatorData * p, size_t
         objectsize)
      : data_(*p)
   {
      data_.object_size = objectsize;
      data_.next_free.clear ();
   }

   SimplePoolAllocator::SimplePoolAllocator (PoolAllocatorData * p)
      : data_ (*p)
   {
   }

   SimplePoolAllocator * SimplePoolAllocator::load (PoolAllocatorData * p)
   {
      return new SimplePoolAllocator (p);
   }

   SimplePoolAllocator::~SimplePoolAllocator ()
   {
   }

   void * SimplePoolAllocator::allocate ()
   {
      if (!data_.next_free)
         return 0;
      void * ret = data_.next_free.get ();
      data_.next_free = data_.next_free->next_free;
      return ret;
   }

   void SimplePoolAllocator::free (void * ptr)
   {
      PoolBlockHeader * h = new (ptr) PoolBlockHeader ();
      h->next_free = data_.next_free;
      data_.next_free = h;
   }

   void SimplePoolAllocator::addMem (void * ptr, size_t size)
   {
      const size_t minsize = std::max (
            static_cast<size_t>(data_.object_size), sizeof (PoolBlockHeader));

      //assert (size > minsize);

      size_t remaining = size;
      char * p = static_cast<char*>(ptr);
      while (remaining > minsize)
      {
         PoolBlockHeader * h = reinterpret_cast<PoolBlockHeader *>(p);
         p+= minsize;
         remaining -= minsize;
         h->next_free = data_.next_free;
         data_.next_free = h;
      }
   }

   bool SimplePoolAllocator::isFull () const
   {
      return !data_.next_free;
   }

   size_t SimplePoolAllocator::getObjectSize () const
   {
      return data_.object_size;
   }

   //========================================================================
}
