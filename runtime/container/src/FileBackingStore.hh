#include "BackingStore.hh"

namespace container
{
   //========================================================================

   class FileBackingStore : public BackingStore
   {
      public:
         virtual void * get ();

         virtual size_t getSize () const;

         virtual void syncRange (size_t start, size_t size);

         FileBackingStore (size_t size, bool shared = false);

         virtual ~FileBackingStore ();

      protected:

         int fd_;

         size_t size_;

         void * store_;
   };

   //========================================================================
}
