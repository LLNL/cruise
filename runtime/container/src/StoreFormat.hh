#ifndef CONTAINER_STOREFORMAT_HH
#define CONTAINER_STOREFORMAT_HH

#include "Container-fwd.hh"
#include "UniquePtr.hh"
#include "PoolAllocator.hh"

#include <cstring> // size_t
#include <stdint.h>

#include "TaggedPtr.hh"

namespace container
{
   //=========================================================================

   // fwd
   struct StoreHeader;
   struct ExtentHeader;
   struct ContainerHeader;
   struct SetHeader;


   class StoreFormat
   {
      public:
         StoreFormat (BackingStore & store, StoreLock & lock);

         ~StoreFormat ();

      public:
         enum
         {
            SF_SUCCESS        = 0,
            SF_ERR_EXISTS,
            SF_ERR_STALE,
            SF_ERR_NOSPACE
         };

         enum
         {
            DEFAULT_PAGE_SIZE = 4*1024*1024
         };

      public:
         typedef TaggedPtr<StoreHeader, size_t> StoreHandle;
         typedef TaggedPtr<ExtentHeader, size_t> ExtentHandle;
         typedef TaggedPtr<ContainerHeader, size_t> ContainerHandle;
         typedef TaggedPtr<SetHeader, size_t> SetHandle;

      public:

         bool validate () const;
         
         /// Try loading an existing storage layout
         void resume ();

         /// Create an empty storage layout
         void format (size_t pagesize);

         /// Calculate layout of storage
         void prepare ();


      public:

         // --- Public operations
         // --- these lock ---

         SetHandle findSet (const char * name) const;

         SetHandle createSet (const char * name, size_t
               reserved, bool exclusive);

         int removeSet (const char * name);


         // ------ Container Operations -----

         ContainerHandle createContainer (SetHandle set, const char * name,
               size_t size, int create, int * created);

         ContainerHandle findContainer (SetHandle set, const char * name);

         int removeContainer (SetHandle set, const char * name);

         int readContainer (ContainerHandle o, size_t memcount,
               void * membuf[], size_t memsizes[], size_t filecount,
               uint64_t fileofs[], uint64_t filesizes[], uint64_t * read);

         int writeContainer (ContainerHandle o, size_t memcount,
               const void * membuf[], size_t memsizes[], size_t filecount,
               uint64_t fileofs[], uint64_t filesizes[], uint64_t * written);

         int resizeContainer (ContainerHandle o, size_t newsize);

         int getContainerSize (ContainerHandle o, uint64_t * size) const;

      protected:

         int transferContainer (ContainerHandle o, size_t memcount,
               void * membuf[], size_t memsizes[], size_t filecount,
               uint64_t fileofs[], uint64_t filesizes[], uint64_t *
               transferred, bool write);

         template <typename T>
         inline bool validateHandle (const TaggedPtr<T, size_t> & handle) const;

         void doError (int err) const;

         void checkPool (PoolAllocator * alloc);

      private:

         // -- these don't lock ---

         inline size_t roundToPageSize (size_t size, size_t pagesize) const;

         inline size_t getPageSize () const;

         void resyncHeader ();


    // Return pointer to byte ofs in the storage
    void * getOfs (size_t ofs);

         // -- reserve bytes ; returns false if not enough space
         bool obtainReserve (size_t bytes);

         // -- release reservation
         bool releaseReserve (size_t bytes);

         SetHeader * allocateSetHeader ();
         void freeSetHeader (SetHeader * header);

         ContainerHeader * allocateContainerHeader ();
         void freeContainerHeader (ContainerHeader * header);

         // --- allocation of data blocks ---
         void freeData (void * ptr, size_t size);
         void * allocateData (size_t size, size_t * out);

         // --- Extent allocation ---
         ExtentHeader * allocateExtentHeader ();
         void freeExtentHeader (ExtentHeader * h);

      protected:
         BackingStore & store_;
         StoreLock & lock_;

         // Header pointers
         StoreHeader * header1_;
         StoreHeader * header2_;

         // Allocator
         // UniquePtr<Allocator> alloc_;

         UniquePtr<PoolAllocator> alloc_page_;
         UniquePtr<PoolAllocator> alloc_set_;
         UniquePtr<PoolAllocator> alloc_container_;
         UniquePtr<PoolAllocator> alloc_extent_;
   };

   //=========================================================================
}

#endif
