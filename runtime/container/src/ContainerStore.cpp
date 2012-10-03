#include "ContainerStore.hh"
#include "BackingStore.hh"
#include "StoreLock.hh"
#include "StoreFormat.hh"

#include "FileBackingStore.hh"
#include "FakeStoreLock.hh"
#include "Util.hh"
#include "ContainerSet.hh"

namespace container
{
   //========================================================================

   ContainerStore::ContainerStore (const char * )
      : store_ (new FileBackingStore (64*1024*1024, false)),
        lock_ (new FakeStoreLock ()),
        format_ (new StoreFormat (*store_, *lock_))
   {

   }

   ContainerStore::~ContainerStore ()
   {
   }

   StoreFormat & ContainerStore::getFormat ()
   {
      return *format_;
   }

   const StoreFormat & ContainerStore::getFormat () const
   {
      return *format_;
   }

   int ContainerStore::available (cs_off_t * available) const
   {
      NOT_IMPLEMENTED;
   }

   ContainerSet * ContainerStore::setOpen (const char * prefix)
   {
      return new ContainerSet (*this, prefix);
   }

   ContainerSet * ContainerStore::setCreate (const char * prefix,
              size_t reserve, bool exclusive)
   {
      return new ContainerSet (*this, prefix, reserve, exclusive);
   }
   
   int ContainerStore::setRemove (const char * prefix)
   {
      return format_->removeSet (prefix);
   }

   //========================================================================
}
