#ifndef CONTAINER_RANGEITERATOR_HH
#define CONTAINER_RANGEITERATOR_HH

namespace container
{
   //========================================================================

   /**
    * Range iterator (for mem,file description ranges).
    * Compatible with ByteWalker
    *
    * TODO: POint to a /const/ array of sizes?
    */
   template <typename PTRT, typename SIZET>
   class RangeIterator : public SafeBool<RangeIterator<PTRT, SIZET> >
   {
      public:
         typedef PTRT   PTRTYPE;
         typedef SIZET  SIZETYPE;

         RangeIterator ()
            : cur_start_ (0), last_start_ (0), cur_size_ (0)
         {
         }

         RangeIterator (size_t count, PTRTYPE * starts,
               SIZETYPE * sizes)
            : cur_start_ (starts), last_start_ (cur_start_ + count),
              cur_size_ (sizes)
         {
         }

         bool boolean_test () const
         {
            return cur_start_ && cur_start_ != last_start_;
         }

         RangeIterator & operator ++ ()
         {
            ++cur_start_;
            ++cur_size_;
         }

         PTRTYPE get () const
         {
            return *cur_start_;
         }

         SIZETYPE getSize () const
         {
            return *cur_size_;
         }

      protected:
         PTRTYPE * cur_start_;
         PTRTYPE * const last_start_;
         SIZETYPE * cur_size_;
   };


   //========================================================================
}

#endif
