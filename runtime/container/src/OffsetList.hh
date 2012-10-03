#ifndef CONTAINER_OFFSETLINKED_HH
#define CONTAINER_OFFSETLINKED_HH

#include "OffsetPtr.hh"

namespace container
{
   //========================================================================

   template <typename T>
   struct OffsetList;

   template <typename T>
   struct OLIterator;

   template <typename T>
   struct OffsetLink
   {
      protected:
         OffsetPtr<T> next;
         OffsetPtr<T> prev;

         friend class OffsetList<T>;
         friend class OLIterator<T>;
   };

   template <typename T>
   struct OLIterator
   {
      public:

         /*OLIterator ()
            : current_ (0)
         {
         } */

         OLIterator (const OLIterator & other)
            : current_ (other.current_)
         {
         }

         OLIterator (T * c)
            : current_ (c)
         {
         }

         T & operator * ()
         { return *current_; }

         const T & operator * () const
         { return *current_; }

         OLIterator operator ++ (int )
         {
            OLIterator c = *this;
            ++(*this);
            return c;
         }

         OLIterator & operator ++ ()
         {
            current_ = current_->next.get();
            return *this;
         }

         bool operator == (const OLIterator & other) const
         {
            return this->current_ == other.current_;
         }

         bool operator != (const OLIterator & other) const
         { return ! this->operator == (other); }

         T * operator -> ()
         { return current_; }

         const T * operator -> () const
         { return current_; }


      protected:
         T * current_;
   };

   template <typename T>
   struct OffsetList
   {
      public:

         typedef OLIterator<T> iterator;

         void pushBack (T & item);

         void remove (T & item);


         // -- initialize memory. Clears list 
         void initialize ()
         {
            clear ();
         }

         // -- Clear all entries
         void clear ()
         {
            header_.first.clear ();
            header_.last.clear ();
         }

         bool empty () const
         {
            return !header_.first;
         }

         void push_front (T & item)
         {
            item.next = header_.first;
            item.prev.clear ();

            header_.first = &item;
            if (!header_.last)
               header_.last = &item;
         }

         void insert_after (T & item, T & newitem)
         {
            newitem.prev = &item;
            newitem.next = item.next;
            item.next = &newitem;
            if (newitem.next)
            {
               newitem.next->prev = &newitem;
            }
            else
            {
               header_.last = &newitem;
            }
         }

         void erase (iterator & i)
         {
            erase (*i);
         }

         void erase (T & item)
         {
            if (item.prev)
            {
               item.prev->next = item.next;
            }
            else
            {
               header_.first = item.next;
            }

            if (item.next)
            {
               item.next->prev = item.prev;
            }
            else
            {
               header_.last = item.prev;
            }
         }


         iterator begin ()
         { return iterator (header_.first.get()); }

         iterator end ()
         { return iterator (0); }

         /**
          * Return the first entry in the list satisfying FUNC (entry)
          */
         template <typename FUNC>
         iterator find (const FUNC & f)
         {
            iterator I = begin ();
            while (I!=end())
            {
               if (f (*I))
                  break;
               ++I;
            }
            return I;
         }

      protected:
         struct
         {
            OffsetPtr< T > first;
            OffsetPtr< T > last;
         } header_;
   };

   //========================================================================


   //========================================================================
}

#endif
