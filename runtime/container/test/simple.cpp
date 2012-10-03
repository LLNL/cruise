#include "container.h"

#include <iostream>
#include <cstdlib>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE cstore
#include <boost/test/unit_test.hpp>


using namespace container;
using namespace std;

// ----------------------------------------------------------------------
// ----------------------------------------------------------------------
// ----------------------------------------------------------------------

struct Validator
{
   Validator () :
      current_ (6)
   {
   }

   void generate (void * ptr, size_t size)
   {
      unsigned char * p = static_cast<unsigned char*>(ptr);
      for (size_t i=0; i<size; ++i)
      {
         *p++ = next ();
      }
   }

   bool validate (const void * ptr, size_t size)
   {
      const unsigned char * p = static_cast<const unsigned char*>(ptr);
      bool ok = true;
      for (size_t i=0; i<size; ++i)
      {
         const unsigned char cur = *p++;
         const unsigned char expected = next ();

         if (cur != expected)
         {
            BOOST_CHECK_EQUAL ((int) cur, (int)expected);
            ok = false;
         }
      }
      return ok;
   }

   unsigned char next ()
   {
      if (++current_ == 13*13)
         current_ = 0;
      return current_;
   }

   unsigned char current_;
};

typedef Validator Generator;

// ----------------------------------------------------------------------
// ----- Test Fixtures --------------------------------------------------
// ----------------------------------------------------------------------

/**
 * Test fixture which initializes the lib
 */
struct Fixture_Store
{
   cs_store_handle_t store_;

   Fixture_Store ()
   {
      BOOST_REQUIRE_EQUAL (cs_store_init (0, &store_), (int) CS_SUCCESS);
   }

   ~Fixture_Store ()
   {
      BOOST_CHECK_EQUAL (cs_store_finalize (store_), (int) CS_SUCCESS);
   }

};

/**
 * Same as above, but we precreate a set called "test"
 */
struct Fixture_Store_Set : public Fixture_Store
{
   cs_set_handle_t set_;

   Fixture_Store_Set ()
   {
      int created = 0;
      set_ = 0;

      /* try to open non-existing set */
      BOOST_CHECK_EQUAL (cs_store_set_open (store_, "test",
               &set_), (int) CS_NOSUCHSET);

      /* Retrying create in non-exclusive mode should work */
      BOOST_REQUIRE_EQUAL (cs_store_set_create (store_, "test",
               4*1024*1024, 0, &set_), (int) CS_SUCCESS);

   }

   ~Fixture_Store_Set ()
   {
      BOOST_CHECK_EQUAL (cs_set_close (set_), (int) CS_SUCCESS);
   }
};


struct Fixture_Store_Set_Container : public Fixture_Store_Set
{
   cs_container_handle_t container_;

   Fixture_Store_Set_Container ()
   {
      const cs_off_t tsize = 3*1024*1024+66;
      int create = 0;

      BOOST_CHECK_NE (cs_set_container_open (set_, "testcont",
               1*1024*1024, 0, &create, &container_), (int) CS_SUCCESS);

      // required because otherwise we'll free an invalid handle
      BOOST_REQUIRE_EQUAL (cs_set_container_open (set_, "testcont",
               tsize, 1, &create, &container_), (int) CS_SUCCESS);

      cs_off_t size = 0;
      BOOST_CHECK_EQUAL (cs_container_size (container_, &size), (int) CS_SUCCESS);
      BOOST_CHECK_EQUAL (size, tsize);
   }

   ~Fixture_Store_Set_Container ()
   {
      BOOST_CHECK_EQUAL (cs_container_close (container_), (int) CS_SUCCESS);
   }
};

// ----------------------------------------------------------------------
// ----------------------------------------------------------------------
// ----------------------------------------------------------------------

/*
 * Try initializing library and releasing it.
 */
BOOST_FIXTURE_TEST_CASE (test_store_init, Fixture_Store)
{
}


/**
 * Same, but create a set (really a test for the fixture)
 */
BOOST_FIXTURE_TEST_CASE (test_create_set, Fixture_Store_Set)
{
}


/**
 * Test set creation (open, create, create_exclusive)
 */
BOOST_FIXTURE_TEST_CASE (test_create_set_2, Fixture_Store_Set)
{
    // non-existing set
    const char * setname = "test2";
    const size_t setsize = 699;
    cs_set_handle_t set;

    /* try to open non-existing set */
    BOOST_CHECK_EQUAL (cs_store_set_open (store_, setname,
                &set), (int) CS_NOSUCHSET);

    /* Now create the set */
    BOOST_CHECK_EQUAL (cs_store_set_create (store_, setname,
                setsize, 1, &set), (int) CS_SUCCESS);

    /* Retrying create in exclusive mode should fail */
    int ret = cs_store_set_create (store_, setname,
                setsize, 1, &set);
    BOOST_CHECK_EQUAL (ret, (int) CS_EXISTS);

    if (ret == CS_SUCCESS)
    {
        /* Close set and try to reopen in non-exclusive mode */
        BOOST_CHECK_EQUAL (cs_set_close (set), (int) CS_SUCCESS);
    }

    /* Retrying create in exclusive mode should fail */
    ret = cs_store_set_create (store_, setname,
                setsize, 0, &set);
    BOOST_CHECK_EQUAL (ret, (int) CS_SUCCESS);

    if (ret == CS_SUCCESS)
    {
        BOOST_CHECK_EQUAL (cs_set_close (set), (int) CS_SUCCESS);
    }
}


/**
 * Same, but create a container as well.
 */
BOOST_FIXTURE_TEST_CASE (test_create_container, Fixture_Store_Set)
{
   int create = 0;
   cs_container_handle_t container;
   const cs_off_t tsize = 1024*1024 + 66;

   // required because otherwise we'll free an invalid handle
   BOOST_REQUIRE_EQUAL (cs_set_container_open (set_, "test",
            tsize, 1, &create, &container), (int) CS_SUCCESS);

   cs_off_t size = 0;
   BOOST_CHECK_EQUAL (cs_container_size (container, &size), (int) CS_SUCCESS);
   BOOST_CHECK_EQUAL (size, tsize);


   BOOST_CHECK_EQUAL (cs_container_close (container), (int) CS_SUCCESS);
}

/**
 * Test reading/writing container
 */
BOOST_FIXTURE_TEST_CASE (test_container_rw, Fixture_Store_Set_Container)
{
   cs_off_t size = 0;

   // we need the container size to do the read/write test
   BOOST_REQUIRE_EQUAL (cs_container_size (container_, &size), (int) CS_SUCCESS);

   cs_off_t todo = size;
   cs_off_t done = 0;
   char buf[255];

   while (todo)
   {
      const void * ptr = buf;
      size_t memsize = std::min (todo, sizeof (buf));
      cs_off_t fileofs = done;
      cs_off_t filesize = memsize;

      cs_off_t written = 0;

      BOOST_CHECK_EQUAL (cs_container_write (container_,
               1, &ptr, &memsize,
               1, &fileofs, &filesize,
               &written), (int) CS_SUCCESS);
      BOOST_CHECK_EQUAL (written, filesize);

      todo -= memsize;
      done += memsize;
   }

   todo = size;
   done = 0;

   while (todo)
   {
      void * ptr = buf;
      size_t memsize = std::min (todo, sizeof (buf));
      cs_off_t fileofs = done;
      cs_off_t filesize = memsize;

      cs_off_t read = 0;

      BOOST_CHECK_EQUAL (cs_container_read (container_,
               1, &ptr, &memsize,
               1, &fileofs, &filesize,
               &read), (int) CS_SUCCESS);
      BOOST_CHECK_EQUAL (read, filesize);

      todo -= memsize;
      done += memsize;
   }
}

/**
 * Test reading/writing container with validation
 */
BOOST_FIXTURE_TEST_CASE (container_rw_validate, Fixture_Store_Set_Container)
{
   cs_off_t size = 0;

   // we need the container size to do the read/write test
   BOOST_REQUIRE_EQUAL (cs_container_size (container_, &size), (int) CS_SUCCESS);

   cs_off_t todo = size;
   cs_off_t done = 0;
   char buf[255];

   Generator gen;

   while (todo)
   {
      const void * ptr = buf;
      size_t memsize = std::min (todo, sizeof (buf));

      // generate verify pattern
      gen.generate (buf, memsize);

      cs_off_t fileofs = done;
      cs_off_t filesize = memsize;

      cs_off_t written = 0;

      BOOST_CHECK_EQUAL (cs_container_write (container_,
               1, &ptr, &memsize,
               1, &fileofs, &filesize,
               &written), (int) CS_SUCCESS);
      BOOST_CHECK_EQUAL (written, filesize);

      todo -= memsize;
      done += memsize;
   }

   todo = size;
   done = 0;

   Validator val;

   while (todo)
   {
      void * ptr = buf;
      size_t memsize = std::min (todo, sizeof (buf));

      // clear buffer
      memset (ptr, 0, memsize);

      cs_off_t fileofs = done;
      cs_off_t filesize = memsize;

      cs_off_t read = 0;

      BOOST_CHECK_EQUAL (cs_container_read (container_,
               1, &ptr, &memsize,
               1, &fileofs, &filesize,
               &read), (int) CS_SUCCESS);
      BOOST_CHECK_EQUAL (read, filesize);

      val.validate (ptr, memsize);

      todo -= memsize;
      done += memsize;
   }
}


