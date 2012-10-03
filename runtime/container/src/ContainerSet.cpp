#include "ContainerSet.hh"
#include "Container.hh"
#include "StoreFormat.hh"
#include "ContainerStore.hh"
#include "ContainerException.hh"

namespace container
{
   //========================================================================

   StoreFormat & ContainerSet::getFormat ()
   {
      return parent_.getFormat ();
   }

   const StoreFormat & ContainerSet::getFormat () const
   {
      return parent_.getFormat ();
   }

   ContainerSet::ContainerSet (ContainerStore & parent,
         const char * prefix,
         size_t reserve, bool exclusive)
       : prefix_(prefix), parent_(parent)
   {
       // @TODO: need to fix so create fails if the set already exists
       handle_ = getFormat ().createSet (prefix, reserve, exclusive);
       if (!handle_)
       {
           throw ContainerException (CS_EXISTS);
       }
   }


   ContainerSet::ContainerSet (ContainerStore & parent,
         const char * prefix)
      : prefix_(prefix), parent_ (parent)
   {
       handle_ = getFormat ().findSet (prefix);
       if (!handle_)
       {
          throw ContainerException (CS_NOSUCHSET);
       }
   }

   Container * ContainerSet::containerOpen (const char * name, cs_off_t size,
         int create, int * created)
   {
      return new Container (*this, name, size, create, created);

      /*StoreFormat::ContainerHandle c;

      if (create)
      {
         // c = format_->createContainer (handle_, name, size);
         *created = 1;
      }

      if (!c)
      {
         // c = format_->findContainer (handle_, name);
         *created = 0; 
      }
      

      if (!c)
         return 0;

      // return new Container (h, prefix); */
   }

   int ContainerSet::available (cs_off_t * reserved, cs_off_t * max) const
   {
   }

   int ContainerSet::containerSize (const char * name, cs_off_t * size)
   {
   }

   int ContainerSet::containerRemove (const char * name)
   {
   }

   int ContainerSet::list (cs_container_t * entries, size_t max,
         size_t * actual)
   {
   }

   //========================================================================
}
