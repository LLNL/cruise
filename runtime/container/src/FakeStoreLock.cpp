#include "FakeStoreLock.hh"

namespace container
{
   //========================================================================

   FakeStoreLock::FakeStoreLock ()
      : write_version_ (0)
   {
   }

   size_t FakeStoreLock::lock (bool write)
   {
      write_ = write;
      return write_version_;
   }

   size_t FakeStoreLock::getVersion () const
   {
      return write_version_;
   }

   size_t FakeStoreLock::getAutoVersion () const
   {
      return (write_ ? getVersion() + 1 : getVersion ());
   }

   size_t FakeStoreLock::unlock (bool write)
   {
      if (write)
         ++write_version_;
      return write_version_;
   }


   FakeStoreLock::~FakeStoreLock ()
   {
   }

   //========================================================================
}
