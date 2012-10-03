#ifndef CONTAINER_INTRUSIVEPTR_HH
#define CONTAINER_INTRUSIVEPTR_HH

#include "SafeBool.hh"

namespace container
{
   //========================================================================

   /**
    *
    * Intrusive reference counting smart pointer.
    *
    * Based on boost ref_ptr model (using free functions to manipulate
    * reference count).
    *
    * T must have
    *
    *    intrusive_add_ref (T * );
    *
    *    intrusive_remove_ref (T * );
    *
    * The latter is responsible for deleteing T * if the reference count hits
    * 0.
    */
   template<class T>
   class IntrusivePtr : public SafeBool<IntrusivePtr<T> >
   {
      protected:
         typedef IntrusivePtr<T> SelfType;

      public:

         IntrusivePtr ()
            : ptr_ (0)
         {
         }

         IntrusivePtr (T * ptr, bool inc = true)
            : ptr_ (ptr)
         {
            if (ptr_ && inc)
               intrusive_add_ref (ptr);
         }


         IntrusivePtr (const IntrusivePtr & other)
            : ptr_ (other.get ())
         {
            if (ptr_)
               intrusive_add_ref (ptr);
         }

         ~IntrusivePtr ()
         {
            if (ptr_)
               intrusive_remove_ref (ptr);
         }

         IntrusivePtr & operator = (const IntrusivePtr & other)
         {
            SelfType (other).swap (*this);
            return *this;
         }

         IntrusivePtr & operator = (T * ptr)
         {
            SelfType (rhs).swap (*this);
            return *this;
         }

         void reset()
         {
            SelfType ().swap (*this);
         }

         void reset( T * rhs )
         {
            SelfType (rhs).swap (*this);
         }

         T * get() const
         {
            return ptr_;
         }

         T & operator * () const
         {
            assert (ptr_);
            return *ptr_;
         }

         T * operator-> () const
         {
            assert (ptr_);
            return ptr_;
         }

         bool boolean_logic () const
         {
            return ptr_;
         }

         void swap (IntrusivePtr & other)
         {
            T * tmp = ptr_;
            ptr_ = other.ptr_;
            other.ptr_ = tmp;
         }

      private:
         T * ptr_;
   };


   //========================================================================
}

#endif

