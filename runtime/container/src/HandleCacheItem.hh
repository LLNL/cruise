#ifndef CONTAINER_HANDLECACHEITEM_HH
#define CONTAINER_HANDLECACHEITEM_HH

namespace container
{
   //========================================================================

   class HandleCacheItem
   {
      public:

         /** Called when the handle needs to be validated */
         virtual int revalidate () = 0;

         virtual ~HandleCacheItem ();
   };
   //========================================================================
}

#endif
