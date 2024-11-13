#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <functional>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace utils
{
class Parallelize
{
  public:
   // Call callback n times with according range (from 0 to n). Upper bound not included
   static void range(u64 threads_count, u64 n, std::function<void(u64 t_i, u64 begin, u64 end)> callback);
   // Same as range, but starts as many threads as hardware allows and joins them again
   static void parallelRange(u64 n, std::function<void(u64, u64)>);
};
}  // namespace utils
}  // namespace leanstore
