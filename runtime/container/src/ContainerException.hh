#ifndef CONTAINER_CONTAINEREXCEPTION_HH
#define CONTAINER_CONTAINEREXCEPTION_HH

#include <exception>
#include <string>

namespace container
{
   //========================================================================

   class ContainerException : public std::exception
   {
      public:

         ContainerException (int errcode, const char * msg);

         ContainerException (int errcode);

         virtual const char * what () const throw ();

         int getError () const
         { return error_; }

         virtual ~ContainerException () throw ();

      protected:

         void buildMsg () const;

      protected:
         int error_;

         mutable std::string msg_;
   };

   //========================================================================
}

#endif
