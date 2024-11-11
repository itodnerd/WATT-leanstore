#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "BufferManager.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
using Time = decltype(std::chrono::high_resolution_clock::now());
BufferManager::PageProviderThread::PageProviderThread(u64 t_i, BufferManager* bf_mgr): id(t_i), bf_mgr(*bf_mgr), 
         async_write_buffer(bf_mgr->ssd_fd, PAGE_SIZE, FLAGS_write_buffer_size,
            [&](PID pid) -> Partition& {return bf_mgr->getPartition(pid);}, &nextup_bfs){
         };
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::PageProviderThread::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, bf_mgr.dram_pool_size);
   COUNTERS_BLOCK() {PPCounters::myCounters().touched_bfs_counter++;}
   return bf_mgr.bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
void BufferManager::PageProviderThread::set_thread_config(){
   if (FLAGS_pin_threads) {
      utils::pinThisThread(std::max(FLAGS_worker_threads, FLAGS_creator_threads) + FLAGS_wal + id);
   } else {
      utils::pinThisThread(FLAGS_wal + id);
   }
   std::string thread_name("pp_" + std::to_string(id));
   CPUCounters::registerThread(thread_name);
   // https://linux.die.net/man/2/setpriority
   if (FLAGS_root) {
      posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
   }
   pthread_setname_np(pthread_self(), thread_name.c_str());
   leanstore::cr::CRManager::global->registerMeAsSpecialWorker();
}
// -------------------------------------------------------------------------------------
bool BufferManager::PageProviderThread::childInRam(BufferFrame* r_buffer, BMOptimisticGuard& r_guard){
      // Check if child in RAM; Add first hot child to cooling queue
      bool all_children_evicted = true;
      bool picked_a_child_instead = false;
      [[maybe_unused]] Time iterate_children_begin, iterate_children_end;
      COUNTERS_BLOCK() { iterate_children_begin = std::chrono::high_resolution_clock::now(); }
      bf_mgr.getDTRegistry().iterateChildrenSwips(r_buffer->page.dt_id, *r_buffer, [&](Swip<BufferFrame>& swip) {
         all_children_evicted &= swip.isEVICTED();  // Ignore when it has a child in the cooling stage
         if (swip.isHOT()) {
            all_children_evicted = false;
            return false;
         }
         r_guard.recheck();
         return true;
      });
      COUNTERS_BLOCK()
      {
         iterate_children_begin = std::chrono::high_resolution_clock::now();
         PPCounters::myCounters().iterate_children_ms +=
             (std::chrono::duration_cast<std::chrono::microseconds>(iterate_children_end - iterate_children_begin).count());
      }
      return !all_children_evicted;
}

// -------------------------------------------------------------------------------------
std::pair<double, double> BufferManager::PageProviderThread::findThresholds()
{
   volatile double min = 500000;
   volatile double next_min = 500000;
   volatile int i=0;
   while(i< FLAGS_watt_samples) {
      jumpmuTry()
      {
         while(i< FLAGS_watt_samples) {
            BufferFrame& r_buffer = randomBufferFrame();
            BMOptimisticGuard r_guard(r_buffer.header.latch);

            if(bf_mgr.pageIsNotEvictable(&r_buffer)){continue;}

            if(childInRam(&r_buffer, r_guard)){continue;}
            i+=1;
            double value = r_buffer.header.tracker.getValue();
            if(value < min){
               next_min = min;
               min=value;
            }
            else if (value < next_min){
               next_min = value;
            }
         }
         jumpmu_break;
      }
      jumpmuCatch(){};
   }
   double calculated_min = min;
   double second_value = min*1.3;
   if(i>1){
      calculated_min = (min + next_min) / 2.0;
      second_value = next_min;
   }
   return {calculated_min, second_value};
}
/***
 * Main Idea: check random BufferFrame (prioritize from nextBufferFrames list), if
 * NonEvictable: skip
 * Evictable and Clean: directly Evict
 * Evictable and Dirty: collect for group flush and add to nextBufferFrames list.
 * @param min
 * @param partitionID
 * @param async_write_buffer
 * @param nextBufferFrames
 */
void BufferManager::PageProviderThread::evictPages(std::pair<double, double> min)
{
   u64 evictions = freed_bfs_batch.free_list->free_bfs_limit - freed_bfs_batch.free_list->counter;
   if(evictions > 1000){
      evictions = 1000;
   }
   const u64 toEvict = evictions;
   volatile u64 evictedPages = 0;
   volatile u64 fails = 0;

   WATT_TIME curr_time = BufferFrame::Header::Tracker::globalTrackerTime.load();
   checkGoodBufferFrames(min, curr_time);
   const u32 batch = 100;
   BufferFrame* frames[batch];
   while(evictedPages < toEvict && fails < 50*toEvict){
      for (u32 j=0; j<batch; j++) {
         frames[j] = getNextBufferFrame();
         __builtin_prefetch(frames[j]);
      }
      for (u32 j=0; j<batch; j++) {
         fails++;
         jumpmuTry()
         {
            // Select and check random BufferFrame
            BufferFrame& r_buffer = *frames[j];

            BMOptimisticGuard r_guard(r_buffer.header.latch);

            if(bf_mgr.pageIsNotEvictable(&r_buffer)){jumpmu_continue;}
            // r_guard.recheck();
            double value = r_buffer.header.tracker.getValue(curr_time);
            if(value > min.first){
               if(value < min.second)
                  second_chance_bfs.push_back(&r_buffer);
               jumpmu_continue;
            }
            // If has children: Evict children, too? (children freq usually < parent freq)
            // Right now: abort if one child is not evicted
            // Pick one or All Childs for eviction (Problem with Partitioning, thats why not direct evict)?
            if(childInRam(&r_buffer, r_guard)){jumpmu_continue;}
            // r_guard.recheck();

            // If clean: Evict directly;
            // Else: Add to async write buffer
            if (!r_buffer.isDirty()) {
               bf_mgr.evict_bf(freed_bfs_batch, r_buffer, r_guard);
               evictedPages++;
               if (freed_bfs_batch.size() > 100) {
                  freed_bfs_batch.push();
               }
               fails--;
               jumpmu_continue;
            }
            // If Async Write Buffer is full, flush
            async_write_buffer.ensureNotFull();
            {
               BMExclusiveGuard ex_guard(r_guard);
               r_buffer.header.is_being_written_back = true;
            }
            {
               BMSharedGuard s_guard(r_guard);
               PID wb_pid = r_buffer.header.pid;
               if (FLAGS_out_of_place) {
                  [[maybe_unused]] u64 p_i = bf_mgr.getPartitionID(r_buffer.header.pid);
                  wb_pid = bf_mgr.getPartition(r_buffer.header.pid).nextPID();
                  paranoid(bf_mgr.getPartitionID(r_buffer.header.pid) == p_i);
                  paranoid(bf_mgr.getPartitionID(wb_pid) == p_i);
               }
               async_write_buffer.add(&r_buffer, wb_pid);
            }
         }
         jumpmuCatch(){};
      }
   }
   // Finally flush
   async_write_buffer.flush();
   if(freed_bfs_batch.size()){
      freed_bfs_batch.push();
   }
   pages_evicted += evictedPages;
   return;
}
void BufferManager::PageProviderThread::checkGoodBufferFrames(std::pair<double, double> threshold, WATT_TIME curr_time)
{
   if(second_chance_bfs.size() == 0){
      return;
   }
   if(curr_time == last_good_check){
      return;
   }
   last_good_check = curr_time;

   std::vector<BufferFrame*> evictions, keep;
   for(BufferFrame* good : second_chance_bfs){
      double value = good->header.tracker.getValue(curr_time);
      if (value < threshold.first){
         evictions.emplace_back(good);
      }else if(value < threshold.second){
         keep.emplace_back(good);
      }
   }
   second_chance_bfs.swap(keep);
   for(BufferFrame* next: evictions){
      nextup_bfs.emplace_back(next);
   }
}
BufferFrame* BufferManager::PageProviderThread::getNextBufferFrame()
{
   if(nextup_bfs.empty()) {
      return &randomBufferFrame();
   }
   BufferFrame* next = nextup_bfs.back();
   nextup_bfs.pop_back();
   return next;
}
void BufferManager::PageProviderThread::run()
{
   set_thread_config();

   // Do Work
   if(FLAGS_epoch_size < 1){
      FLAGS_epoch_size = 1;
   }
   while (bf_mgr.bg_threads_keep_running) {
      auto& current_free_list = bf_mgr.randomFreeList();
      if (current_free_list.counter >= current_free_list.free_bfs_limit) {
            continue;
      }
      freed_bfs_batch.set_free_list(&current_free_list);
      evictPages(findThresholds());
   }
   bf_mgr.bg_threads_counter--;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
