#ifndef CONTAINER_OFFSETPOINTER_HH
#define CONTAINER_OFFSETPOINTER_HH

#include "SafeBool.hh"
#include "Util.hh"

#include <stdint.h>
#include <assert.h>
#include <cstring>

#include <limits>

namespace container
{
   //========================================================================

   /**
    * Inject reference methods if type is not void
    */

   
   template <typename T, typename T2>
   class OffsetPtrHelper
   {
      public:
         T2 & operator * ()
         { return * (static_cast<T*>(this)->get ()); }

         const T2 & operator * () const
         { return *(static_cast<const T*>(this)->get ()); }
   };

   template <typename T>
   class OffsetPtrHelper<T, void>
   {
   };
 

   /**
    * Smart pointer using relative addressing;
    * It stores the difference between the address of the item pointed to and
    * its own address. This means it can be moved/mapped in another location
    */
   template <typename T>
   class OffsetPtr : public SafeBool< OffsetPtr<T> >,
                     public OffsetPtrHelper< OffsetPtr<T>, T>
   {
      public:

         typedef T BASE;

         typedef std::ptrdiff_t shared_ptrdiff_t;

#define INVALID std::numeric_limits<shared_ptrdiff_t>::max()

         OffsetPtr (T * ptr = 0)
         {
            assert (sizeof(std::ptrdiff_t) <= sizeof (shared_ptrdiff_t));
            reset (ptr);
         }

         // -- needed to prevent auto generated operator =
         OffsetPtr (const OffsetPtr & other)
         {
            reset (other.get());
         }

         template <typename T2>
         OffsetPtr (const OffsetPtr<T2> & other)
         {
            reset (other.get ());
         }

         template <typename T2>
         OffsetPtr & operator = (T2 * ptr)
         {
            reset (ptr);
         }

         OffsetPtr & operator = (T * other)
         {
            reset (other);
         }

         OffsetPtr & operator = (OffsetPtr & other)
         {
            reset (other.get());
         }


         template <typename T2>
         OffsetPtr & operator = (const OffsetPtr<T2> & other)
         {
            reset (other.get ());
            return *this;
         }

         void reset (T * ptr)
         {
            if (!ptr)
            {
               // offset_ == INVALID means the null pointer
               // offset_ == 0 means pointing to ourselves
               offset_ = INVALID;
               return;
            }

            offset_ = ptrDiff ((char*)ptr, reinterpret_cast<char*>(this));
            assert (get () == ptr);
         }

         void clear ()
         {
            offset_ = INVALID;
         }

         T * operator -> ()
         {
            assert (!isNull ());
            return getTyped ();
         }

         const T * operator -> () const
         {
            assert (!isNull ());
            return getTyped ();
         }

         const T * get () const
         { return getTyped (); }

         T * get ()
         { return getTyped (); }

         bool isNull () const
         {
            return offset_ == INVALID;
         }

      public:

          bool boolean_test () const
          {
             return !isNull ();
          }

      protected:

         T * getTyped ()
         {
            return (isNull () ? 0 :
                (T*) ptrAdd (reinterpret_cast<char*>(this), offset_));
         }

         const T * getTyped () const
         {
            return (isNull () ? 0 :
               (const T*)   ptrAdd (reinterpret_cast<const char*>(this), offset_));
         }


      protected:
         shared_ptrdiff_t offset_;

   };

   template <typename T1, typename T2>
   bool operator == (const OffsetPtr<T1> & p1, const OffsetPtr<T2> & p2)
   {
      return p1.get () == p2.get ();
   }


   template <typename T1, typename T2>
   bool operator != (const OffsetPtr<T1> & p1, const OffsetPtr<T2> & p2)
   {
      return !(p1 == p2);
   }

   //========================================================================
}

#endif
