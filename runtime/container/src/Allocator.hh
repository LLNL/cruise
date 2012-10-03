#ifndef CONTAINER_ALLOCATOR_HH
#define CONTAINER_ALLOCATOR_HH

#include <cstring>

namespace container
{
   //========================================================================

   class Allocator
   {
      public:

         /// Create a new allocator
         /*&static Allocator * construct (void * ptr, size_t size);

         static Allocator * reconstruct (void * ptr, size_t size);
         */

         virtual void * allocate (size_t size, size_t minsize, size_t * out) = 0;

         virtual void free (void * ptr, size_t size) = 0;

         virtual size_t available () const = 0;

         virtual ~Allocator ();
   };

   //========================================================================
}

#endif
