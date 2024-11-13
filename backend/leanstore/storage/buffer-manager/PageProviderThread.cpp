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
bool BufferManager::PageProviderThread::checkXMerge(BufferFrame* r_buffer){
      const SpaceCheckResult space_check_res = bf_mgr.getDTRegistry().checkSpaceUtilization(r_buffer->page.dt_id, *r_buffer);
      return space_check_res == SpaceCheckResult::RESTART_SAME_BF || space_check_res == SpaceCheckResult::PICK_ANOTHER_BF;
}
// -------------------------------------------------------------------------------------
std::pair<double, double> BufferManager::PageProviderThread::findThresholds()
{
   volatile double min = 50000, second_min = 50000;
   atomic<u32> valid_tests={0};
   while(valid_tests< FLAGS_watt_samples) {
      jumpmuTry()
      {
         while(valid_tests< FLAGS_watt_samples) {
            BufferFrame& r_buffer = randomBufferFrame();
            BMOptimisticGuard r_guard(r_buffer.header.latch);
            if(bf_mgr.pageIsNotEvictable(&r_buffer)){continue;}
            if(childInRam(&r_buffer, r_guard)){continue;}
            double value = r_buffer.header.tracker.getValue();
            r_guard.recheck();
            if(value < min){
               second_min = min;
               min=value;
            }
            else if (value < second_min){
               second_min = value;
            }
            valid_tests++;
         }
         jumpmu_break;
      }
      jumpmuCatch(){};
   }
   if(valid_tests>1){
      return {(min + second_min)/2.0, second_min};
   }
   return {min, min*1.3};
}
/***
 * Main Idea: check random BufferFrame (prioritize from nextBufferFrames list), if
 * NonEvictable: skip
 * Evictable and Clean: directly Evict
 * Evictable and Dirty: collect for group flush and add to nextBufferFrames list.
 * @param min
 */
// -------------------------------------------------------------------------------------
void BufferManager::PageProviderThread::evictPages(std::pair<double, double> min)
{
   atomic<u64> evictions = freed_bfs_batch.free_list->free_bfs_limit - freed_bfs_batch.free_list->counter;
   if(evictions > 1000){
      evictions = 1000;
   }
   if(evictions < 100){
      evictions = 100;
   }

   atomic<u64> evictedPages = {0}, fails = {0};
   WATT_TIME curr_time = BufferFrame::Header::Tracker::globalTrackerTime.load();
   checkGoodBufferFrames(min, curr_time);
   const u32 batch = 64;
   BufferFrame* frames[batch];
   while(evictedPages < evictions && fails < 5*evictions){
      for (u32 j=0; j<batch; j++) {
         frames[j] = getNextBufferFrame();
         __builtin_prefetch(frames[j]);
      }
      for (u32 j=0; j<batch; j++) {
         BufferFrame& r_buffer = *frames[j];
         fails++;
         jumpmuTry()
         {
            // Select and check random BufferFrame
            BMOptimisticGuard r_guard(r_buffer.header.latch);
            if(bf_mgr.pageIsNotEvictable(&r_buffer)){jumpmu_continue;}
            r_guard.recheck();
            // If has children: Evict children, too? (children freq usually < parent freq)
            // Right now: abort if one child is not evicted
            // Other ideas: Pick one or All Childs for eviction?
            if(childInRam(&r_buffer, r_guard)){jumpmu_continue;}
            r_guard.recheck();
            if(checkXMerge(&r_buffer)){jumpmu_continue;}
            r_guard.recheck();
            double value = r_buffer.header.tracker.getValue(curr_time);
            r_guard.recheck();
            if(value > min.first){
               if(value < min.second)
                  second_chance_bfs.push_back(&r_buffer);
               jumpmu_continue;
            }
            const PID r_buffer_pid = r_buffer.header.pid;
            r_guard.recheck();
            // Prevent evicting a page that already has an IO Frame with (possibly) threads working on it.
            {
               HashTable& io_table = bf_mgr.getIOTable(r_buffer_pid);
               JMUW<std::unique_lock<std::mutex>> io_guard(io_table.ht_mutex);
               if (io_table.lookup(r_buffer_pid)) {
                  jumpmu_continue;
               }
            }
            // If dirty: Add to async write buffer
            // Else: Evict directly;
            if (r_buffer.isDirty()) {
               handleDirty(r_guard, r_buffer, r_buffer_pid);
            } else {
               bf_mgr.evict_bf(freed_bfs_batch, r_buffer, r_guard);
               evictedPages++;
               if (freed_bfs_batch.size() > batch) {
                  freed_bfs_batch.push();
               }
               fails--;
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
   return;
}
void BufferManager::PageProviderThread::handleDirty(leanstore::storage::BMOptimisticGuard& r_guard,
                                                    leanstore::storage::BufferFrame& r_buffer,
                                                    const PID bf_pid)
{
   async_write_buffer.ensureNotFull();
   BMExclusiveGuard ex_guard(r_guard);
   paranoid(!r_buffer.header.is_being_written_back);
   r_buffer.header.is_being_written_back.store(true, std::memory_order_release);
   if (FLAGS_crc_check) {
      r_buffer.header.crc = utils::CRC(r_buffer.page.dt, EFFECTIVE_PAGE_SIZE);
   }

   // TODO: preEviction callback according to DTID
   PID wb_pid = bf_pid;
   if (FLAGS_out_of_place) {
      [[maybe_unused]] u64 p_i = bf_mgr.getPartitionID(bf_pid);
      wb_pid = bf_mgr.getPartition(bf_pid).nextPID();
      paranoid(bf_mgr.getPartitionID(bf_pid) == bf_mgr.getPartitionID(wb_pid));
   }
   async_write_buffer.add(&r_buffer, wb_pid);
}

void BufferManager::PageProviderThread::checkGoodBufferFrames(std::pair<double, double> threshold, WATT_TIME curr_time)
{
   if(second_chance_bfs.size() == 0){
      return;
   }
   /*
   if(curr_time == last_good_check){
      return;
   }
   last_good_check = curr_time;
   */
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
   while (bf_mgr.bg_threads_keep_running) {
      auto& current_free_list = bf_mgr.randomFreeList();
      if (current_free_list.counter >= current_free_list.free_bfs_limit) {
            continue;
      }
      freed_bfs_batch.set_free_list(&current_free_list);
      evictPages(findThresholds());
      COUNTERS_BLOCK() { PPCounters::myCounters().pp_thread_rounds++; }
   }
   bf_mgr.bg_threads_counter--;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
