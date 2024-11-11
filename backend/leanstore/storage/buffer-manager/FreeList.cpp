#include "FreeList.hpp"

#include "Exceptions.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
void FreeList::batchPush(BufferFrame* batch_head, BufferFrame* batch_tail, u64 batch_counter)
{
   do {
      batch_tail->header.next_free_bf = head.load();
   }while (!head.compare_exchange_strong(batch_tail->header.next_free_bf, batch_head));
   counter += batch_counter;
}
// -------------------------------------------------------------------------------------
void FreeList::push(BufferFrame& bf)
{
   paranoid(bf.header.state == BufferFrame::STATE::FREE);
   bf.header.latch.assertNotExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   do {
      bf.header.next_free_bf = head.load();
   } while (!head.compare_exchange_strong(bf.header.next_free_bf, &bf));
   counter++;
}
// -------------------------------------------------------------------------------------
struct BufferFrame& FreeList::tryPop()
{
   BufferFrame *free_bf, *next;
   u16 trys = 0;
   do {
      free_bf = head;
      if (free_bf == nullptr && trys > 1000) {
         jumpmu::jump();
      }
      trys ++;
      next = free_bf->header.next_free_bf;
   } while (free_bf == nullptr || !head.compare_exchange_strong(free_bf, next));

   free_bf->header.next_free_bf = nullptr;
   counter--;
   new_pages++;
   paranoid(free_bf->header.state == BufferFrame::STATE::FREE);
   if(new_pages >= new_pages_per_epoch){
      new_pages = 0;
      leanstore::storage::BufferFrame::Header::Tracker::globalTrackerTime++;
   }
   return *free_bf;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
