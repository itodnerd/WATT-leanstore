#pragma once
#include "BufferFrame.hpp"
#include "Partition.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <libaio.h>

#include <functional>
#include <list>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
class AsyncWriteBuffer
{
  private:
   struct WriteCommand {
      BufferFrame* bf;
      PID pid;
   };
   io_context_t aio_context;
   int fd;
   u64 page_size, batch_max_size, submitted_pages;
   std::function<Partition&(PID)> getPartition;
   std::vector<std::pair<BufferFrame*, PID>> pagesToWrite;
   std::vector<BufferFrame*> pagesWritten;
   std::vector<BufferFrame*>* nextup_bfs;
  public:
   std::unique_ptr<BufferFrame::Page[]> write_buffer;
   std::unique_ptr<WriteCommand[]> write_buffer_commands;
   std::unique_ptr<struct iocb[]> iocbs;
   std::unique_ptr<struct iocb*[]> iocbs_ptr;
   std::unique_ptr<struct io_event[]> events;
   // -------------------------------------------------------------------------------------
   // Debug
   // -------------------------------------------------------------------------------------
   AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size, std::function<Partition&(PID)> getPartition, std::vector<BufferFrame*>* nextup_bfs);
   // Caller takes care of sync
   void ensureNotFull();
   void add(BufferFrame* bf, PID pid);
   void flush();
  private:
   bool full();
   bool empty();
   void handleWritten();
   void submit();
   void waitAndHandle();
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
