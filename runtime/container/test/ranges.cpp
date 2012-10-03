#include "ByteWalker.hh"
#include "RangeIterator.hh"
#include "RangeCombiner.hh"

#include <boost/cstdint.hpp>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE ranges
#include <boost/test/unit_test.hpp>

using boost::uint64_t;

using namespace container;

template <typename ITERATOR>
void testIteratorOutput (ITERATOR i, const typename ITERATOR::PTRTYPE * ptr,
      const typename ITERATOR::SIZETYPE * size)
{
   while (*size)
   {
      BOOST_REQUIRE_MESSAGE (i, "Iterator indicated EOF too soon!");

      BOOST_CHECK_EQUAL (i.getSize(), *size++);
      BOOST_CHECK_EQUAL (i.get(), *ptr++);

      ++i;
   }

   BOOST_CHECK_MESSAGE (!i, "Iterator did not indicate EOF!");
}

template <typename B>
void testByteWalker (B b, const typename B::PTRTYPE * data,
      const typename B::SIZETYPE * remaining,
      const typename B::PTRTYPE * segstart,
      const typename B::SIZETYPE * segsize,
      const typename B::SIZETYPE * ofsinseg)
{
   typename B::SIZETYPE moved = 0;
   while (*remaining)
   {
      BOOST_REQUIRE_MESSAGE (b, "ByteWalker indicate EOF too soon!");
      BOOST_CHECK_EQUAL (b.getData (), *data++);
      BOOST_CHECK_EQUAL (b.getSegmentRemaining (), *remaining++);
      BOOST_CHECK_EQUAL (b.getSegmentStart (), *segstart++);
      BOOST_CHECK_EQUAL (b.getSegmentSize (), *segsize++);
      BOOST_CHECK_EQUAL (b.getOffsetInSegment (), *ofsinseg++);
      BOOST_CHECK_EQUAL (b.getPosition (), moved);

      ++moved;
      ++b;
   }
}

template <typename T>
size_t len (const T * p)
{
   size_t count = 0;
   while (*p++)
      ++count;
   return count;
}

BOOST_AUTO_TEST_CASE( test_RangeIterator )
{
   uint64_t startofs[]      = { 1, 10, 25, 36, 0 };
   uint64_t sizes[]         = { 2, 3,  4,  5,  0 };

   RangeIterator<uint64_t, uint64_t> i1 (len (sizes), &startofs[0], &sizes[0]);

   testIteratorOutput (i1, startofs, sizes);
}


BOOST_AUTO_TEST_CASE (test_ByteWalker)
{
   uint64_t startofs[] = { 100, 110, 120, 0 };
   uint64_t sizes[] =    {   2,   1,   2, 0 };

   uint64_t data[]           = { 100, 101, 110, 120, 121, 0 };
   uint64_t segremaining[]   = {   2,   1,   1,   2,   1, 0 };
   uint64_t segstart[]       = { 100, 100, 110, 120, 120, 0 };
   uint64_t segsize[]        = {   2,   2,   1,   2,   2, 0 };
   uint64_t ofsinseg[]       = {   0,   1,   0,   0,   1, 0 };



   typedef RangeIterator<uint64_t, uint64_t> MyRange;

   MyRange i (len (sizes), startofs, sizes);
   ByteWalker<MyRange> b (i);

   testByteWalker (b, data, segremaining, segstart, segsize, ofsinseg);

}

BOOST_AUTO_TEST_CASE( test_RangeCombiner )
{
   typedef RangeIterator<uint64_t, uint64_t> MyRange;
   typedef RangeCombiner<MyRange,MyRange> MyCombiner;

   uint64_t startofs[]      = { 1, 10, 25, 36, 0 };
   uint64_t sizes[]         = { 2, 3,  4,  5,  0 };

   uint64_t startofs2[]     = { 100, 0 };
   uint64_t sizes2[]        = { 200, 0 };

   uint64_t startofs3[]     = { 0,   100,  200, 0 };
   uint64_t sizes3[]        = { 25,  25,   25,  0 };

   // range 1 combined with range 2
   uint64_t combinedstart[] = { 101, 110, 125, 136, 0 };
   uint64_t combinedsizes[] = { 2,   3,   4,   5,   0 };

   // range 1 combined with range 3
   uint64_t combinedstart2[] = { 1, 10, 100, 111, 0 };
   uint64_t combinedsizes2[] = { 2,  3,   4,   5, 0 };


   {
      BOOST_TEST_MESSAGE("Test R1+R2");
      MyRange i1 (len (sizes), startofs, sizes);
      MyRange i2 (len (sizes2), startofs2, sizes2);
      MyCombiner rc (i1, i2);
      testIteratorOutput (rc, combinedstart, combinedsizes);
   }

   {
      BOOST_TEST_MESSAGE("Test R1+R3");
      MyRange i1 (len (sizes), startofs, sizes);
      MyRange i3 (len (sizes3), startofs3, sizes3);
      MyCombiner rc (i1, i3);
      testIteratorOutput (rc, combinedstart2, combinedsizes2);
   }
}


