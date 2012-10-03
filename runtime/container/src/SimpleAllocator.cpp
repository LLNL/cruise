#include "SimpleAllocator.hh"
#include "Util.hh"

#include <cassert>

namespace container
{
   //========================================================================

   SimpleAllocator::SimpleAllocator (void * ptr, size_t size,
         bool format)
      : header_ (static_cast<AllocHeader *> (ptr))
   {
      assert (format);

      always_assert (size > sizeof (AllocHeader) + sizeof (BlockHeader)+1);

      BlockHeader * b = static_cast<BlockHeader*>
                            (addOfs (ptr, sizeof (AllocHeader)));
      b->size = size - sizeof(AllocHeader) - sizeof (BlockHeader);

      // always recreate for now
      header_->totalsize = size;

      header_->free.initialize ();
      header_->free.push_front (*b);

      header_->available = b->size;
   }


   SimpleAllocator::~SimpleAllocator ()
   {
   }

   inline int64_t sizeDiff (uint64_t a, uint64_t b)
   {
      return (a > b ? int64_t(a - b) : -int64_t(b - a));
   }

   size_t SimpleAllocator::available () const
   {
      return header_->available;
   }

   void SimpleAllocator::splitBlock (BlockHeader * b, size_t size)
   {
      BlockHeader * newheader = static_cast<BlockHeader*>(addOfs (b, size));
      newheader->size = b->size - size;

      assert (newheader->size >= sizeof (BlockHeader));

      header_->free.insert_after (*b, *newheader);
   }

   void * SimpleAllocator::allocate (size_t size, size_t minsize, size_t * out)
   {
      FreeListType::iterator best = header_->free.end();
      BlockHeader * best_prev = 0;
      *out = size;

      //BlockHeader * current = header_->free.get();

      int64_t best_diff = 0;

      FreeListType::iterator current = header_->free.begin ();
      while (current != header_->free.end ())
      {
         if (current->size < size)
            continue;

         if (best != header_->free.end ())
         {
            best = current;
            best_diff = sizeDiff (current->size, size);
         }
         else
         {
            const int64_t diff = sizeDiff (current->size, size);

            if (diff < best_diff)
            {
               best = current;
               best_diff = diff;
            }

            if (!best_diff)
            {
               // block of perfect size; stop search
               break;
            }
         }

         ++current;
      }

      if (current == header_->free.end ())
      {
         // Could not find any block big enough
         return 0;
      }

      if (best_diff > 4*1024)
      {
         splitBlock (&(*current), size);
      }

      *out = current->size;
      header_->available -= current->size;

      header_->free.erase (current);

      return & (*current);
   }

   void SimpleAllocator::free (void * ptr, size_t size)
   {
      BlockHeader * b = static_cast<BlockHeader *> (ptr);
      b->size = size;

      header_->free.push_front (*b);
      header_->available += size;
   }

   //========================================================================
}
