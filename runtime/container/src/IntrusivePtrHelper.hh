#ifndef CONTAINER_INTRUSIVEPTRHELPER_HH
#define CONTAINER_INTRUSIVEPTRHELPER_HH

#include <assert.h>
#include <pthread.h>

namespace container
{
   //========================================================================

   class IntrusivePtrHelper
   {
      protected:
         typedef unsigned int RefType;

      public:
         IntrusivePtrHelper (size_t ref = 0)
            : ref_ (ref)
         {
            pthread_mutex_init (&lock_, 0);
         }

         ~IntrusivePtrHelper ()
         {
            assert (!ref_);
            pthread_mutex_destroy (&lock_);
         }

      public:

         RefType addRef ()
         {
            pthread_mutex_lock (&lock_);
            const RefType newr = ++ref_;
            pthread_mutex_unlock (&lock_);
            return newr;
         }

         RefType removeRef ()
         {
            pthread_mutex_lock (&lock_);
            assert (ref_);
            const RefType newr = --ref_;
            pthread_mutex_unlock (&lock_);
            return newr;
         }

      protected:

         RefType ref_;
         pthread_mutex_t lock_;
   };

#define INTRUSIVE_PTR_HELPER(CLASS) \
   inline void intrusive_add_ref (CLASS * ptr) \
   { \
      ptr->addRef (); \
   } \
   inline void intrusive_remove_ref (CLASS * ptr) \
   { \
      if (!ptr->removeRef ()) \
         delete (ptr); \
   }


   //========================================================================
}

#endif
