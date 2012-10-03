#ifndef CONTAINER_RANGECOMBINER_HH
#define CONTAINER_RANGECOMBINER_HH

#include "SafeBool.hh"
#include "ByteWalker.hh"

namespace container
{
   //========================================================================

   /**
    * Takes two RangeIterators and combines them into a single RangeIterator
    * which iterate over the ranges in the second iterator indicated by the
    * pointers of the first. The pointer returned is the pointer of the second
    * iterator for this ranges.
    *
    * So, if PRIM has 10,5 and 20,10 then this iterator will go move forward
    * 10 bytes into SEC, return contiguous ranges from SEC for 5 bytes, then
    * skip to byte 20 in SEC and return 10 bytes of data.
    *
    * Used to combine file range descriptions with the container extent
    * information.
    *
    * PRIM and SEC have to have the same SIZETYPE;
    * PRIM::PTRTYPE has to be convertible to SEC::SIZETYPE;
    */
   template <typename PRIM, typename SEC>
   class RangeCombiner : public SafeBool <RangeCombiner<PRIM, SEC> >
   {
      public:
         typedef typename PRIM::SIZETYPE SIZETYPE;
         typedef typename SEC::PTRTYPE   PTRTYPE;

         RangeCombiner (PRIM prim, SEC sec)
            : prim_ (prim), sec_ (sec)
         {
            // -- forward the secund to the offset indicated by the ptr of the
            // first.
            if (*this)
               sec_.skipTo (prim_.getData ());
         }

         PTRTYPE get ()
         {
            return sec_.getData ();
         }

         SIZETYPE getSize () const
         {
            return std::min (prim_.getSegmentRemaining (),
                  sec_.getSegmentRemaining ());
         }

         void operator ++ ()
         {
            const SIZETYPE skip = getSize ();
            prim_ += skip;
            if (prim_)
            {
               sec_.skipTo (prim_.getData ());
               assert (sec_);
            }
         }

         bool boolean_test () const
         {
            return prim_ && sec_;
         }

      protected:
         ByteWalker<PRIM> prim_;
         ByteWalker<SEC> sec_;
   };

   //========================================================================
}

#endif
