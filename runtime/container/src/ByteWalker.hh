#ifndef CONTAINER_MEMITERATOR_HH
#define CONTAINER_MEMITERATOR_HH

#include "SafeBool.hh"
#include "Util.hh"

#include <algorithm>
#include <assert.h>

namespace container
{
   //========================================================================

   /**
    * Helper class to do byte moves over segments/extents
    *
    * Does not follow iterator spec.
    *
    * BASE needs to have:
    *
    *    typedef PTRTYPE
    *    typedef SIZETYPE
    *
    *    operator ++
    *
    *    PTRTYPE  get ()
    *    SIZETYPE getSize () const
    *
    *    convert to boolean type
    *
    *
    */
   template <typename BASE>
   class ByteWalker : public SafeBool<ByteWalker<BASE> >
   {
      public:
         typedef typename BASE::PTRTYPE PTRTYPE;
         typedef typename BASE::SIZETYPE SIZETYPE;

         ByteWalker (const BASE & b)
            : base_ (b), cur_ofs_ (0), moved_ (0)
         {
         }

         bool boolean_test () const
         {
            return base_ && (cur_ofs_ < base_.getSize());
         }

      public:

         // Return total size of current segment
         SIZETYPE getSegmentSize () const
         {
            return base_.getSize ();
         }

         // Return start offset of current segment
         PTRTYPE getSegmentStart () const
         {
            return base_.get ();
         }

         /// Return the number of bytes of the current segment we used
         SIZETYPE getOffsetInSegment () const
         {
            return cur_ofs_;
         }

         /// Return the number of bytes remaining in this segment
         SIZETYPE getSegmentRemaining () const
         {
            return getSegmentSize () - getOffsetInSegment ();
         }

         // Move to next segment
         void nextSegment ()
         {
            assert (base_);
            moved_ += getSegmentSize () - cur_ofs_;
            cur_ofs_ = 0;
            ++base_;
         }

         void operator ++ ()
         {
            *this += 1;
         }

         void operator += (SIZETYPE bytes)
         {
            SIZETYPE to_go = bytes;
            while (to_go)
            {
               assert (base_);
               const SIZETYPE thismove =
                  std::min (to_go, getSegmentRemaining ());

               to_go -= thismove;
               cur_ofs_ += thismove;
               moved_ += thismove;

               if (!getSegmentRemaining ())
               {
                  nextSegment ();
               }
            }
         }

         /**
          * Move to byte x in the segment list
          * NOTE: this considers segment sizes, not the segment starting
          * offset.
          */
         void skipTo (SIZETYPE pos)
         {
            assert (moved_ <= pos);
            *this += (pos - moved_);
         }

         /**
          * Return how many bytes we moved in the segment list
          */
         SIZETYPE getPosition () const
         {
            return moved_;
         }

         /**
          * Return a pointer to the current element in the segment
          */
         PTRTYPE getData ()
         {
            return ptrAdd (base_.get (), getOffsetInSegment ());
         }

      protected:

         // Segment provider
         BASE base_;

         // offset in current segment
         SIZETYPE cur_ofs_;

         // Number of bytes we moved forward in the segment list
         SIZETYPE moved_;
   };

   //========================================================================
}

#endif
