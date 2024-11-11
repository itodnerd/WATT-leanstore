#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
struct FreeList {
   const u64 free_bfs_limit;
   const u64 new_pages_per_epoch;
   atomic<BufferFrame*> head = {nullptr};
   std::atomic<u64> counter = {0};
   std::atomic<u64> new_pages = {0};
   std::atomic<bool> is_page_provided = {false};
   // -------------------------------------------------------------------------------------
   BufferFrame& tryPop();
   void batchPush(BufferFrame* head, BufferFrame* tail, u64 counter);
   void push(BufferFrame& bf);
   FreeList(u64 free_bfs_limit, u64 dram_pool_size): free_bfs_limit(free_bfs_limit), new_pages_per_epoch(std::max((u64) 1, (u64) dram_pool_size / FLAGS_epoch_size)){}
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore

// -------------------------------------------------------------------------------------
