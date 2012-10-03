#include "StoreFormat.hh"

#include "BackingStore.hh"
#include "StoreLock.hh"
#include "ScopedStoreLock.hh"
#include "SimpleAllocator.hh"
#include "SimplePoolAllocator.hh"

#include <stdint.h>
#include "OffsetPtr.hh"
#include "Util.hh"
#include "OffsetList.hh"
#include "SafeBool.hh"
#include "RangeIterator.hh"
#include "ByteWalker.hh"
#include "RangeCombiner.hh"
#include "StoreFormatException.hh"

#include <algorithm>

#include "container-types.h"

namespace container
{
   //========================================================================

   struct ExtentHeader
   {
      OffsetPtr<void>         data;
      OffsetPtr<ExtentHeader> next;
      uint64_t                size;
   };

   struct ContainerHeader : public OffsetLink<ContainerHeader>
   {
      char                      name[256];
      OffsetPtr<ExtentHeader>   extent;
      uint64_t                  size;
   };

   struct SetHeader : public OffsetLink<SetHeader>
   {
      typedef OffsetList<ContainerHeader>  ContainerListType;

      char                        name[256];
      uint64_t                    reserved;
      ContainerListType           objects;
   };

   static const char * StoreHeader_Magic = "CSNLS";  // 5 + null

   struct StoreHeader
   {
      char magic[6];
      uint64_t store_size;    // raw store size

      uint64_t page_size;     // basic allocation unit (for align)

      uint64_t free;
      uint64_t reserved;

      typedef OffsetList<SetHeader> SetListType;
      SetListType  sets;

      PoolAllocatorData  alloc_page_data;
      PoolAllocatorData  alloc_set_data;
      PoolAllocatorData  alloc_container_data;
      PoolAllocatorData  alloc_extent_data;

      // ---------------

      void setMagic ();

      bool checkMagic () const;
   };

   bool StoreHeader::checkMagic () const
   {
      return !memcmp (&magic[0], StoreHeader_Magic, sizeof (magic));
   }

   void StoreHeader::setMagic ()
   {
      memcpy (&magic[0], StoreHeader_Magic, sizeof(magic));
   }
   
   //========================================================================

   template <typename T>
   struct NameCompare
   {
      NameCompare (const char * ptr)
         : name_ (ptr)
      {
      }

      bool operator () (const T & item) const
      {
         return !strcmp (name_, item.name);
      }

      protected:
      const char * name_;
   };

   //========================================================================

   /**
    * Simplify stepping through the extent list.
    * Fulfills requirements for ByteWalker.
    */
   struct ExtentIterator : public SafeBool<ExtentIterator>
   {
      public:
         typedef void *   PTRTYPE;
         typedef uint64_t SIZETYPE;

         ExtentIterator ()
            : extent_ (0)
         {
         }

         ExtentIterator (ExtentHeader * p)
            : extent_ (p)
         {
         }

         bool boolean_test () const
         { return extent_; }

         /** Size of the extent */
         uint64_t getSize () const
         {
            assert (*this);
            return extent_->size;
         }

         void * get () const
         {
            assert (*this);
            return extent_->data.get();
         }

         ExtentIterator & operator ++ ()
         {
            assert (*this);
            extent_ = extent_->next.get();
         }

      protected:
         ExtentHeader * extent_;
   };

   //========================================================================

   template <typename T>
   bool StoreFormat::validateHandle (const TaggedPtr<T, size_t> & handle) const
   {
      return (handle.getTag () == lock_.getVersion ());
   }

   size_t StoreFormat::roundToPageSize (size_t bytes, size_t pagesize) const
   {
      return ((bytes + pagesize - 1)/ pagesize ) * pagesize;
   }

   size_t StoreFormat::getPageSize () const
   {
      return header1_->page_size;
   }

   bool StoreFormat::obtainReserve (size_t bytes)
   {
      if (header1_->free < bytes)
         return false;

      header1_->free -= bytes;

      resyncHeader ();
   }

   bool StoreFormat::releaseReserve (size_t bytes)
   {
      header1_->free += bytes;
      resyncHeader ();
      return true;
   }

   void StoreFormat::resyncHeader ()
   {
      *header2_ = *header1_;
   }

   StoreFormat::SetHandle StoreFormat::findSet (const char * name) const
   {
      ScopedStoreLock l (lock_, false);

      StoreHeader::SetListType::iterator found =
         header1_->sets.find (NameCompare<SetHeader> (name));

      if (found != header1_->sets.end())
         return SetHandle (&(*found), lock_.getAutoVersion ());
      else
         return SetHandle ();
   }

   SetHeader * StoreFormat::allocateSetHeader ()
   {
      checkPool (alloc_set_.get ());
      return reinterpret_cast<SetHeader*>(alloc_set_->allocate ());
   }

   void StoreFormat::freeSetHeader (SetHeader * ptr)
   {
      alloc_set_->free (ptr);
   }

   ContainerHeader * StoreFormat::allocateContainerHeader ()
   {
      checkPool (alloc_container_.get ());
      return reinterpret_cast<ContainerHeader*>(alloc_container_->allocate
            ());
   }

   void StoreFormat::freeContainerHeader (ContainerHeader * ptr)
   {
      alloc_container_->free (ptr);
   }

   ExtentHeader * StoreFormat::allocateExtentHeader ()
   {
      checkPool (alloc_extent_.get ());
      return reinterpret_cast<ExtentHeader*>
         (alloc_extent_->allocate ());
   }

   void StoreFormat::freeExtentHeader (ExtentHeader * ptr)
   {
      alloc_extent_->free (ptr);
   }

   StoreFormat::SetHandle StoreFormat::createSet (const char * name,
         size_t reserved, bool exclusive)
   {
      ScopedStoreLock l (lock_, true);

      // -- see if it already exists
      StoreHeader::SetListType::iterator found =
         header1_->sets.find (NameCompare<SetHeader> (name));

      if (found != header1_->sets.end())
      {
         if (exclusive)
            return SetHandle ();

         // + 1 since we took a write lock which will automatically increment
         // the version when we return from this function.
         return SetHandle (&(*found), lock_.getAutoVersion ());
      }

      // -- does not exist, we create it
 
      SetHeader * newset = allocateSetHeader ();
      memset (newset, 0, sizeof (SetHeader));
      strncpy_safe (newset->name, name, sizeof (newset->name));
      newset->reserved = reserved;
      newset->objects.initialize ();

      header1_->sets.push_front (*newset);

      resyncHeader ();

      return SetHandle (newset, lock_.getAutoVersion ());
   }

   void StoreFormat::freeData (void * ptr, size_t size)
   {
      assert (size == getPageSize ());
      alloc_page_->free (ptr);
   }

   void * StoreFormat::allocateData (size_t size,
         size_t * out)
   {
      // @TODO: Need to optimize here to reduce the number of extents
      *out = getPageSize ();
      return alloc_page_->allocate ();
   }


   StoreFormat::ContainerHandle StoreFormat::createContainer (SetHandle set,
         const char * name, size_t size, int create, int * created)
   {
      ScopedStoreLock l (lock_, true);

      SetHeader::ContainerListType::iterator found =
         set->objects.find (NameCompare<ContainerHeader>(name));

      if (found != set->objects.end ())
      {
         *created = 0;
         return ContainerHandle (& (*found), lock_.getAutoVersion ());
      }

      // Container does not exist

      if (!create)
      {
         *created = 0;
         return ContainerHandle ();
      }
 
      ContainerHeader * newobject = allocateContainerHeader ();
      memset (newobject, 0, sizeof (ContainerHeader));
      strncpy_safe (newobject->name, name, sizeof (newobject->name));
      newobject->extent.clear ();

      OffsetPtr<ExtentHeader> * prev = &newobject->extent;

      size_t todo = size;
      while (todo)
      {
         size_t datasize;
         void * data = allocateData (todo, &datasize);
         assert (data);

         ExtentHeader * extent = allocateExtentHeader ();
         extent->data = data;
         extent->size = datasize;
         extent->next.clear ();

         // link previous extent to this one
         *prev = extent;
         prev = &extent->next;

         todo -= std::min(todo, datasize);
      }

      newobject->size = size;

      set->objects.push_front (*newobject);

      resyncHeader ();

      *created = 1;

      return ContainerHandle (newobject, lock_.getAutoVersion ());
   }

   StoreFormat::ContainerHandle StoreFormat::findContainer (SetHandle set, const char * name)
   {
      ScopedStoreLock l (lock_, false);
      
      // TODO
      /*if (!validateHandle (o))
         return SF_ERR_STALE;
         */

      SetHeader::ContainerListType::iterator found =
         set->objects.find (NameCompare<ContainerHeader>(name));

      if (found == set->objects.end ())
         return ContainerHandle ();
      else
         return ContainerHandle (& (*found), lock_.getAutoVersion ());
   }


   StoreFormat::StoreFormat (BackingStore & store, StoreLock & lock)
      : store_ (store), lock_ (lock)
	{
	   // Always format for now
	   format (DEFAULT_PAGE_SIZE);
	}

   StoreFormat::~StoreFormat ()
   {
   }


   void StoreFormat::doError (int err) const
   {
      throw err;
   }

   void StoreFormat::resume ()
   {
      ScopedStoreLock l (lock_, true);

      /** TODO: determine storage layout and set header pointers */

      assert (header1_);
      assert (header2_);

      if (!header1_->page_size)
         doError (CS_INVALID_FORMAT);

      alloc_page_.reset (SimplePoolAllocator::load
            (&header1_->alloc_page_data));
      alloc_set_.reset (SimplePoolAllocator::load
            (&header1_->alloc_set_data));
      alloc_container_.reset (SimplePoolAllocator::load
            (&header1_->alloc_container_data));
      alloc_container_.reset (SimplePoolAllocator::load
            (&header1_->alloc_extent_data));
   }

   void StoreFormat::format (size_t pagesize)
   {
      ScopedStoreLock l (lock_, true);

      // -- calculate layout based on default page size
      void * raw = store_.get ();
      size_t raw_size = store_.getSize ();

      size_t headersize_padded = roundToPageSize (sizeof (StoreHeader), pagesize);

      void * pagestart = addOfs (raw, headersize_padded);
      // Header at the front and back
      size_t pagecount = (raw_size - 2*headersize_padded) / pagesize;

      header1_ = static_cast<StoreHeader *> (raw);
      header2_ = static_cast<StoreHeader *> (addOfs (pagestart, pagesize*pagecount));

      header1_->setMagic ();
      header1_->store_size = raw_size;
      header1_->page_size = pagesize;
      header1_->free = pagecount * pagesize;
      header1_->reserved = 0;

      header1_->sets.clear ();

      
      alloc_page_.reset (new SimplePoolAllocator 
            (&header1_->alloc_page_data, header1_->page_size));
      alloc_set_.reset (new SimplePoolAllocator
            (&header1_->alloc_set_data, sizeof (SetHeader)));
      alloc_container_.reset (new SimplePoolAllocator
            (&header1_->alloc_container_data, sizeof (ContainerHeader)));
      alloc_extent_.reset (new SimplePoolAllocator
            (&header1_->alloc_extent_data, sizeof (ExtentHeader)));

      assert (pagesize >= sizeof (SetHeader));
      assert (pagesize >= sizeof (ContainerHeader));
      assert (pagesize >= sizeof (ExtentHeader));

      // -- add space to page allocator
      alloc_page_->addMem (pagestart, pagecount*pagesize);

      resyncHeader ();
   }


   int StoreFormat::readContainer (ContainerHandle o, size_t memcount,
               void * membuf[], size_t memsizes[], size_t filecount,
               uint64_t fileofs[], uint64_t filesizes[], uint64_t * read)
   {
      return transferContainer (o, memcount, membuf, memsizes,
            filecount, fileofs, filesizes, read, false);
   }

   int StoreFormat::writeContainer (ContainerHandle o, size_t memcount,
               const void * membuf[], size_t memsizes[], size_t filecount,
               uint64_t fileofs[], uint64_t filesizes[], uint64_t * written)
   {

      return transferContainer (o, memcount, const_cast<void **>(membuf),
            memsizes,
            filecount, fileofs, filesizes, written, true);
   }

   int StoreFormat::transferContainer (ContainerHandle o, size_t memcount,
               void * membuf[], size_t memsizes[], size_t filecount,
               uint64_t fileofs[], uint64_t filesizes[], uint64_t *
               transferred, bool write)
   {
      // We only need a read lock for the metadata here since we're modifying
      // container _data_ but not container _metadata_.
      ScopedStoreLock l (lock_, false);

      if (!validateHandle (o))
         return SF_ERR_STALE;

      typedef RangeIterator<void *, size_t> MemRangeIterator;
      typedef RangeIterator<uint64_t, uint64_t> FileRangeIterator;
      typedef RangeCombiner<FileRangeIterator, ExtentIterator> CombinedRange;

      // Combine extent and filerange
      ExtentIterator extents (o->extent.get ());
      FileRangeIterator fileranges (filecount, fileofs, filesizes);
      CombinedRange filei (fileranges, extents);

      // Mem interator
      MemRangeIterator memi (memcount, membuf, memsizes);

      // Construct byte addressible version
      ByteWalker<MemRangeIterator> membi (memi);
      ByteWalker<CombinedRange> filebi (filei);

      size_t done = 0;

      // Now we have two byte iterators, one returning pointers to memory
      // data, one returning pointers to the file data. Now we just walk over
      // both copying to or from until we're done.
      while (membi)
      {
         assert (filebi && membi);

         const size_t thistransfer = std::min (membi.getSegmentRemaining (),
               filebi.getSegmentRemaining ());

         assert (thistransfer);

         if (write)
            memcpy (filebi.getData (), membi.getData(), thistransfer);
         else
            memcpy (membi.getData (), filebi.getData (), thistransfer);

         filebi += thistransfer;
         membi += thistransfer;
         done += thistransfer;
      }

      *transferred = done;

      assert (!membi && !filebi);
      return CS_SUCCESS;
   }

   int StoreFormat::removeContainer (SetHandle set, const char * name)
   {
      ScopedStoreLock l (lock_, true);

      if (!validateHandle (set))
         return SF_ERR_STALE;
   }

   int StoreFormat::resizeContainer (ContainerHandle o, uint64_t newsize)
   {
      ScopedStoreLock l (lock_, true);

      if (!validateHandle (o))
         return SF_ERR_STALE;

      return CS_NOTIMPLEMENTED;
   }

   int StoreFormat::getContainerSize (ContainerHandle o, uint64_t * size) const
   {
      ScopedStoreLock l (lock_, false);

      if (!validateHandle (o))
         return SF_ERR_STALE;

      *size = o->size;
      return CS_SUCCESS;
   }


   int StoreFormat::removeSet (const char * prefix)
   {
      ScopedStoreLock l (lock_, true);
      return CS_NOTIMPLEMENTED;
   }

   void StoreFormat::checkPool (PoolAllocator * alloc)
   {
      if (!alloc->isFull ())
         return;

      void * newpage = alloc_page_->allocate ();
      if (!newpage)
            doError (SF_ERR_NOSPACE);

      assert (newpage);

      alloc->addMem (newpage, alloc_page_->getObjectSize ());
      assert (!alloc->isFull ());
   }

   //========================================================================
}
