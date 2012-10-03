#ifndef CONTAINER_TAGGEDPTR_HH
#define CONTAINER_TAGGEDPTR_HH

#include "SafeBool.hh"

namespace container
{
   //=======================================================================

   template <typename PTR, typename TAG>
   class TaggedPtr : public SafeBool<TaggedPtr<PTR,TAG> >
   {
      protected:
         typedef PTR * PTRHELPER;
      public:
         TaggedPtr (PTR * ptr, TAG t)
            : ptr_ (ptr), tag_ (t)
         {
         }

         TaggedPtr ()
            : ptr_ (PTRHELPER ()),
            tag_ (TAG ())
         {
         }

         TAG getTag () const
         {
            return tag_;
         }

         PTR * get () const
         {
            return ptr_;
         }

         PTR * operator -> ()
         { return ptr_; }

         const PTR * operator -> () const
         { return ptr_; }

         bool boolean_test () const
         { return ptr_; }

      protected:
         PTR * ptr_;
         TAG tag_;
   };

   //=======================================================================
}

#endif
