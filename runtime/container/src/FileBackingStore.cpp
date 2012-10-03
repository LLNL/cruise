#include "FileBackingStore.hh"

#include <string>
#include <cassert>

#include <sys/mman.h>
#include <stdlib.h>

namespace container
{
   //========================================================================

   FileBackingStore::FileBackingStore (size_t size, bool shared)
      : fd_(-1), size_ (size)
   {
      if (shared)
      {
         char filename[] = "container_XXXXXX";
         fd_ = mkstemp (filename);
         assert (fd_ >= 0);
      }
      else
      {
         fd_ = -1;
      }

      store_ = mmap (0, size_, PROT_READ|PROT_WRITE,
            MAP_SHARED | (shared ? 0 : MAP_ANONYMOUS),
            (shared ? fd_ : -1),
            0);
      assert (store_ != MAP_FAILED);

   }

   FileBackingStore::~FileBackingStore ()
   {
      if (fd_ >= 0)
         close (fd_);
   }

   void * FileBackingStore::get ()
   {
      return store_;
   }

   size_t FileBackingStore::getSize () const
   {
      return size_;
   }

   void FileBackingStore::syncRange (size_t start, size_t size)
   {
      msync (static_cast<char*>(store_) + start, size, MS_SYNC);
   }

   //========================================================================
}
