#include "BackingStore.hh"

namespace container
{
   //========================================================================

   BackingStore::~BackingStore ()
   {
   }

   void BackingStore::sync ()
   {
      syncRange (0, getSize ());
   }

   //========================================================================
}
