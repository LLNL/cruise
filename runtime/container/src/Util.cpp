#include "Util.hh"

#include <cstdlib>
#include <cstdio>
#include <assert.h>

using namespace std;

namespace container
{
   //========================================================================

   void notImplementedHelper ()
   {
      assert (0 && "Not implemented!");
   }

   void assert_fail ()
   {
      fprintf (stderr, "Always_assert failed!n");
      exit (1);
   }

   char * strncpy_safe (char * dest, const char * source, size_t totalsize)
   {
      strncpy (dest, source, totalsize);
      // strncpy pads with null-bytes;
      if (dest[totalsize])
      {
         /* buffer was too small */
         dest[totalsize]=0;
         return 0;
      }
      return dest;
   }

   //========================================================================
}
