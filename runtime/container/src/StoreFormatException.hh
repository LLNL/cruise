#ifndef CONTAINER_STOREFORMATEXCEPTION_HH
#define CONTAINER_STOREFORMATEXCEPTION_HH


#include "ContainerException.hh"
#include <string>

namespace container
{
   //========================================================================

   class StoreFormatException : public ContainerException
   {
      public:

         StoreFormatException (int errcode)
            : ContainerException (errcode)
         {
         }

   };

   //========================================================================
}

#endif
