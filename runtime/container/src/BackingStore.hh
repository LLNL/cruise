#ifndef CONTAINER_BACKINGSTORE_HH
#define CONTAINER_BACKINGSTORE_HH

#include <cstddef>

using std::size_t;

namespace container
{
   //========================================================================

   /**
    * TODO: Consider adding 'expose' - show to other processes but don't flush
    * to persistent storage.
    */
   struct BackingStore
   {
      public:

         virtual void * get () = 0;

         virtual size_t getSize () const = 0;

         virtual void sync ();

         virtual void syncRange (size_t start, size_t size) = 0;

         virtual ~BackingStore ();

   };

   //========================================================================
}


#endif
