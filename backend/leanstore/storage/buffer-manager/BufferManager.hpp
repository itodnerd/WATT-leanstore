#pragma once
#include "Units.hpp"
#include "BMPlainGuard.hpp"
#include "BufferFrame.hpp"
#include "DTRegistry.hpp"
#include "FreeList.hpp"
#include "Partition.hpp"
#include "Swip.hpp"
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
#include "AsyncWriteBuffer.hpp"
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <sys/mman.h>

#include <cstring>
#include <list>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
class LeanStore;
namespace profiling
{
class BMTable;
}
namespace storage
{
// -------------------------------------------------------------------------------------
/*
 * Swizzle a page:
 * 1- bf_s_lock global bf_s_lock
 * 2- if it is in cooling stage:
 *    a- yes: bf_s_lock write (spin till you can), remove from stage, swizzle in
 *    b- no: set the state to IO, increment the counter, hold the mutex, p_read
 * 3- if it is in IOFlight:
 *    a- increment counter,
 */
class BufferManager
{
  private:
   friend class leanstore::LeanStore;
   friend class leanstore::profiling::BMTable;
   // -------------------------------------------------------------------------------------
   BufferFrame* bfs;
   // -------------------------------------------------------------------------------------
   const int ssd_fd;
   // -------------------------------------------------------------------------------------
   // Free  Pages
   const u8 safety_pages = 10;               // we reserve these extra pages to prevent segfaults
   const u64 dram_pool_size;                       // total number of dram buffer frames
   atomic<u64> ssd_freed_pages_counter = 0;  // used to track how many pages did we really allocate
   // -------------------------------------------------------------------------------------
   // For cooling and inflight io
   const u64 partitions_count;
   const u64 partitions_mask;
   const u64 max_partition_size;
   static FreeList free_list;

   std::vector<std::unique_ptr<Partition>> partitions;

  private:
   // -------------------------------------------------------------------------------------
   // Threads managements
   void pageProviderThread(u64 p_begin, u64 p_end);  // [p_begin, p_end)
   atomic<u64> bg_threads_counter = {0};
   atomic<bool> bg_threads_keep_running = {true};
   // -------------------------------------------------------------------------------------
   // Misc
   Partition& randomPartition();
   BufferFrame& randomBufferFrame();
   Partition& getPartition(PID);
   u64 getPartitionID(PID);

  public:
   // -------------------------------------------------------------------------------------
   BufferManager(s32 ssd_fd);
   ~BufferManager();
   // -------------------------------------------------------------------------------------
   BufferFrame& allocatePage();
   BufferFrame& resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value);
   void coolPage(BufferFrame& bf);
   void reclaimPage(BufferFrame& bf);
   // -------------------------------------------------------------------------------------
   /*
    * Life cycle of a fix:
    * 1- Check if the pid is swizzled, if yes then store the BufferFrame address
    * temporarily 2- if not, then posix_check if it exists in cooling stage
    * queue, yes? remove it from the queue and return the buffer frame 3- in
    * anycase, posix_check if the threshold is exceeded, yes ? unswizzle a random
    * BufferFrame (or its children if needed) then add it to the cooling stage.
    */
   // -------------------------------------------------------------------------------------
   void readPageSync(PID pid, u8* destination);
   void readPageAsync(PID pid, u8* destination, std::function<void()> callback);
   void fDataSync();
   // -------------------------------------------------------------------------------------
   void stopBackgroundThreads();
   void writeAllBufferFrames();
   std::unordered_map<std::string, std::string> serialize();
   void deserialize(std::unordered_map<std::string, std::string> map);
   // -------------------------------------------------------------------------------------
   u64 getPoolSize() { return dram_pool_size; }
   DTRegistry& getDTRegistry() { return DTRegistry::global_dt_registry; }
   u64 consumedPages();
   BufferFrame& getContainingBufferFrame(const u8*);  // get the buffer frame containing the given ptr address
   struct FreedBfsBatch {
      BufferFrame *freed_bfs_batch_head = nullptr, *freed_bfs_batch_tail = nullptr;
      u64 freed_bfs_counter = 0;
      // -------------------------------------------------------------------------------------
      void reset()
      {
         freed_bfs_batch_head = nullptr;
         freed_bfs_batch_tail = nullptr;
         freed_bfs_counter = 0;
      }
      // Publish Free Pages to partition
      void push()
      {
         free_list.batchPush(freed_bfs_batch_head, freed_bfs_batch_tail, freed_bfs_counter);
         reset();
      }
      // -------------------------------------------------------------------------------------
      u64 size() { return freed_bfs_counter; }
      // Add empty bufferframes to list;
      void add(BufferFrame& bf)
      {
         bf.header.next_free_bf = freed_bfs_batch_head;
         if (freed_bfs_batch_head == nullptr) {
            freed_bfs_batch_tail = &bf;
         }
         freed_bfs_batch_head = &bf;
         freed_bfs_counter++;
         // -------------------------------------------------------------------------------------
      }
   };

   void nonDirtyEvict(Partition& partition,
                      std::_List_iterator<BufferFrame*>& bf_itr,
                      BufferFrame& bf,
                      BMOptimisticGuard& guard,
                      FreedBfsBatch& evictedOnes);
   void cool_pages();
   void second_phase(AsyncWriteBuffer& async_write_buffer,
                     volatile u64 p_i,
                     const s64 pages_to_iterate_partition,
                     Partition& partition,
                     FreedBfsBatch& freed_bfs_batch);
   using Time = decltype(std::chrono::high_resolution_clock::now());
   void third_phase(AsyncWriteBuffer& async_write_buffer,
                    volatile u64 p_i,
                    Partition& partition,
                    const s64 pages_to_iterate_partition,
                    FreedBfsBatch& freed_bfs_batch,
                    Time& phase_3_end);
   void wait_for_start();
};                                                    // namespace storage
// -------------------------------------------------------------------------------------
class BMC
{
  public:
   static BufferManager* global_bf;
};
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
