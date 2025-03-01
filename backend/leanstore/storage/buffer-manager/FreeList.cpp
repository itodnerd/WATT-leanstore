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
   for(u64 i =0; i< batch_counter; i++){
      counter ++;
   }
}
// -------------------------------------------------------------------------------------
void FreeList::push(BufferFrame& bf)
{
   assert(bf.header.state == BufferFrame::STATE::FREE);
   bf.header.latch.assertNotExclusivelyLatched();
   do {
      bf.header.next_free_bf = head.load();
   } while (!head.compare_exchange_strong(bf.header.next_free_bf, &bf));
   counter++;
}
// -------------------------------------------------------------------------------------
struct BufferFrame& FreeList::pop(JMUW<std::unique_lock<std::mutex>>* lock)
{
   BufferFrame *free_bf, *next;
   u16 trys = 0;
   do {
      free_bf = head;
      if (free_bf == nullptr) {
         if(trys > 1000){
            // cout << "dry2" <<endl;
            if(lock != nullptr) {
               (*lock)->unlock();
            }
            jumpmu::jump();
         }
         trys ++;
         continue;
      }
      next = free_bf->header.next_free_bf;
   } while (free_bf == nullptr || !head.compare_exchange_strong(free_bf, next));

   free_bf->header.next_free_bf = nullptr;
   counter--;
   free_bf->header.latch.assertNotExclusivelyLatched();
   assert(free_bf->header.state == BufferFrame::STATE::FREE);

   return *free_bf;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
