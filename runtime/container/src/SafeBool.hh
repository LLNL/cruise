#ifndef CONTAINER_SAFEBOOL_HH
#define CONTAINER_SAFEBOOL_HH

namespace container
{
   //========================================================================

   template <typename T>
   class SafeBool
   {
      protected:
         typedef void (SafeBool::*bool_type) () const;
         void this_type_does_not_support_comparisons () const {}

         SafeBool ()
         {}

         SafeBool (const SafeBool &)
         {}

         SafeBool & operator=(const SafeBool &)
         { return *this; }

         ~SafeBool ()
         {}

      public:
         operator bool_type() const
         {
            return (static_cast<const T *>(this))->boolean_test () ?
                &SafeBool ::this_type_does_not_support_comparisons 
              : 0;
         }
    };

   template <typename T, typename U>
   void operator == (const SafeBool <T> & lhs, const SafeBool <U> & rhs)
   {
         lhs.this_type_does_not_support_comparisons();
         return false;
   }

   template <typename T,typename U>
   void operator != (const SafeBool <T> & lhs, const SafeBool <U> & rhs)
   {
         lhs.this_type_does_not_support_comparisons();
         return false;
   }


   //========================================================================
}

#endif
