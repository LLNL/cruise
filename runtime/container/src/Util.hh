#ifndef CONTAINER_UTIL_HH
#define CONTAINER_UTIL_HH

#include <cstring>

namespace container
{
   //========================================================================
   

   inline void * addOfs (void * ptr, std::ptrdiff_t ofs)
   {
      return static_cast<char *>(ptr) + ofs;
   }

   inline const void * addOfs (const void * ptr, std::ptrdiff_t ofs)
   {
      return static_cast<const char *>(ptr) + ofs;
   }

   /// Enable pointer arithmetic, including for void pointers
   /// (treating void * as char *) and integer based values
   template <typename T>
   inline T ptrAdd (T p, std::ptrdiff_t count)
   {
      return p + count;
   }

   /// Enable pointer arithmetic, including for void pointers
   /// (treating void * as char *)
   template <>
   inline void * ptrAdd (void * p, std::ptrdiff_t count)
   {
      return addOfs (p, count);
   }


   template <typename T>
   inline std::ptrdiff_t ptrDiff (T * p1, T * p2)
   {
      return p1-p2;
   }

   template <>
   inline std::ptrdiff_t ptrDiff (void * p1, void * p2)
   {
      return static_cast<char*>(p1)-static_cast<char*>(p2);
   }

   void assert_fail ();

   inline void always_assert (bool b)
   {
      if (b)
         return;
      assert_fail ();
   }


   /**
    * Copy up to totalsize - 1 bytes to dest. Guarantees to NULL-terminate.
    * Returns 0 if buffer wasn't big enough
    */
   char * strncpy_safe (char * dest, const char * source, size_t totalsize);


   void notImplementedHelper ();

#define NOT_IMPLEMENTED notImplementedHelper ()

   template <typename T>
   inline T roundUpToPage (T value, T pagesize)
   {
      return ((value + pagesize - 1) / pagesize ) * pagesize;
   }

   template <typename T>
   inline T roundDownToPage (T value, T pagesize)
   {
      return (value / pagesize) * pagesize;
   }

   //========================================================================
}

#endif
