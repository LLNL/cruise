#include "Container.hh"
#include "ContainerSet.hh"
#include "ContainerException.hh"

namespace container
{
   //========================================================================

   Container::Container (ContainerSet & parent, const char * name,
         cs_off_t size, int create, int * created)
      : parent_ (parent),
        name_ (name)
   {
      handle_ = getFormat ().createContainer (
                   parent_.getHandle (), name, size, create, created);
      if (!handle_ && !create)
         throw ContainerException (CS_NOSUCHCONTAINER);
   }

   StoreFormat & Container::getFormat ()
   {
      return parent_.getFormat ();
   }

   const StoreFormat & Container::getFormat () const
   {
      return parent_.getFormat ();
   }
   int Container::read (size_t memcount, void * buf[], size_t sizes[],
         size_t filecount, cs_off_t fileofs[], cs_off_t filesizes[],
         cs_off_t * transferred)
   {
      return getFormat().readContainer (getHandle (),
            memcount, buf, sizes, filecount, fileofs, filesizes, transferred);
   }

   int Container::write (size_t memcount, const void * buf[], size_t sizes[],
         size_t filecount, cs_off_t fileofs[], cs_off_t filesizes[],
         cs_off_t * transferred)
   {
      return getFormat().writeContainer (getHandle (),
            memcount, buf, sizes, filecount, fileofs, filesizes, transferred);
   }

   int Container::size (cs_off_t * size) const
   {
      return getFormat().getContainerSize (getHandle (), size);
   }

   //========================================================================
}
