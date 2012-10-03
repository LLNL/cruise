#include "OffsetPtr.hh"
#include "OffsetList.hh"

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE offset
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <cstdlib>

using namespace container;

using namespace std;

template <typename T>
void fillRandom (T & t)
{
   char * s = reinterpret_cast<char*>(&t);
   for (size_t i=0; i<sizeof(T); ++i)
      *s++ = random ();

}

template <typename T>
void testOfs ()
{
   T val1;
   T val2;

   fillRandom (val1);
   fillRandom (val2);

   OffsetPtr<T> p1;
   OffsetPtr<T> p2;

   BOOST_CHECK (!p1);
   BOOST_CHECK (!p2);

   BOOST_CHECK_NO_THROW (p1 = 0);
   BOOST_CHECK (!p1);

   p1 = &val1;

   BOOST_CHECK_EQUAL (p1.get(), &val1);
   
   BOOST_CHECK_EQUAL (*p1, val1);
   BOOST_CHECK (p1 != p2);
   p2 = p1;
   BOOST_CHECK_EQUAL (p1, p2);
   BOOST_CHECK_EQUAL (*p2, val1);
   BOOST_CHECK_EQUAL (p2.get(), &val1);
   BOOST_CHECK_EQUAL (p1.get(), p2.get());
   BOOST_CHECK (p1);
   BOOST_CHECK (p2);
   BOOST_CHECK_NO_THROW (p2.clear ());
   BOOST_CHECK (!p2);
   BOOST_CHECK_EQUAL (p2.get (), static_cast<T*>(0));
}

BOOST_AUTO_TEST_CASE (test_offsetptr)
{
   testOfs<char>();
   testOfs<double> ();
}


