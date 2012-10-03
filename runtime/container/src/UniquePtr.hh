#ifndef CONTAINER_UNIQUEPTR_HH
#define CONTAINER_UNIQUEPTR_HH

#include <assert.h>

namespace container
{

   namespace
   {
      
      template<class T> inline void checked_delete(T * x)
      {
         // intentionally complex - simplification causes regressions
         typedef char type_must_be_complete[ sizeof(T)? 1: -1 ];
         (void) sizeof(type_must_be_complete);
         delete x;
      }

   }


   template <typename T>
   struct UniquePtr
   {
      public:
         UniquePtr (T * ptr = 0)
            : ptr_ (ptr)
         {
         }

         ~UniquePtr ()
         {
            checked_delete (ptr_);
         }

         void reset (T * ptr = 0)
         {
            assert (!ptr || (ptr_ != ptr));

            checked_delete (ptr_);
            ptr_ = ptr;
         }

         T * operator -> ()
         {
            assert (ptr_);
            return ptr_;
         }

         T & operator * ()
         {
            assert (ptr_);
            return *ptr_;
         }

         const T & operator * () const
         {
            assert (ptr_);
            return *ptr_;
         }

         const T * operator -> () const
         {
            assert (ptr_);
            return ptr_;
         }

         const T * get () const
         {
            return ptr_;
         }

         T * get ()
         {
            return ptr_;
         }

      protected:
         T * ptr_;
   };

}

#endif
