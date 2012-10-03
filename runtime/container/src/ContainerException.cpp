#include "ContainerException.hh"
#include <cassert>
#include <cstdio>

namespace container
{
   //========================================================================

   ContainerException::ContainerException (int err)
      : error_ (err)
   {
   }

   ContainerException::ContainerException (int err, const char * s)
      : error_ (err), msg_ (s)
   {
   }


   const char * ContainerException::what () const throw ()
   {
      if (msg_.empty ())
         buildMsg ();

      return msg_.c_str ();
   }

   void ContainerException::buildMsg () const
   {
      char buf[255];
      const int len = snprintf (buf, sizeof(buf)-1, "container: err %i",
            getError ());

      assert (len > 0);
      assert (len < sizeof(buf)-1);

      msg_ = buf;
   }

   ContainerException::~ContainerException () throw ()
   {
   }

   //========================================================================
}
