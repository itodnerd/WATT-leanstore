#pragma once
#include "Exceptions.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace buffermanager
{
// -------------------------------------------------------------------------------------
#define PAGE_GUARD_HEADER    \
  BufferFrame* bf = nullptr; \
  OptimisticGuard bf_s_lock; \
  bool moved = false;

#define PAGE_GUARD_UTILS                                        \
  void kill() { moved = true; }                                 \
  T& ref() { return *reinterpret_cast<T*>(bf->page.dt); }       \
  T* ptr() { return reinterpret_cast<T*>(bf->page.dt); }        \
  Swip<T> swip() { return Swip<T>(bf); }                        \
  T* operator->() { return reinterpret_cast<T*>(bf->page.dt); } \
  bool hasBf() const { return bf != nullptr; }

// Objects of this class must be thread local !
template <typename T>
class ExclusivePageGuard;
template <typename T>
class SharedPageGuard;
template <typename T>
class OptimisticPageGuard
{
 protected:
  OptimisticPageGuard(OptimisticLatch& swip_version) : bf_s_lock(OptimisticGuard(swip_version)), moved(false) { jumpmu_registerDestructor(); }
  OptimisticPageGuard(OptimisticGuard read_guard, BufferFrame* bf) : bf(bf), bf_s_lock(std::move(read_guard)), moved(false)
  {
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
  // -------------------------------------------------------------------------------------

 public:
  // -------------------------------------------------------------------------------------
  PAGE_GUARD_HEADER
  bool manually_checked = false;
  // -------------------------------------------------------------------------------------
  OptimisticPageGuard() : bf(nullptr), bf_s_lock(nullptr, 0), moved(true) { jumpmu_registerDestructor(); }  // use with caution
  // -------------------------------------------------------------------------------------
  // Move constructor
  OptimisticPageGuard(OptimisticPageGuard&& other)
      : bf(other.bf), bf_s_lock(std::move(other.bf_s_lock)), moved(other.moved), manually_checked(other.manually_checked)
  {
    other.moved = true;
    jumpmu_registerDestructor();
  }
  // Copy constructor
  OptimisticPageGuard(OptimisticPageGuard& other)
      : bf(other.bf), bf_s_lock(other.bf_s_lock), moved(other.moved), manually_checked(other.manually_checked)
  {
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
  static OptimisticPageGuard manuallyAssembleGuard(OptimisticGuard read_guard, BufferFrame* bf)
  {
    return OptimisticPageGuard(std::move(read_guard), bf);
  }
  // -------------------------------------------------------------------------------------
  // I: Root case
  static OptimisticPageGuard makeRootGuard(OptimisticLatch& swip_version) { return OptimisticPageGuard(swip_version); }
  // -------------------------------------------------------------------------------------
  // I: Lock coupling
  OptimisticPageGuard(OptimisticPageGuard& p_guard, Swip<T>& swip)
      : bf(&BMC::global_bf->resolveSwip(p_guard.bf_s_lock, swip.template cast<BufferFrame>())), bf_s_lock(OptimisticGuard(bf->header.lock))
  {
    assert(!(p_guard.bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT));
    assert(!p_guard.moved);
    jumpmu_registerDestructor();
    p_guard.recheck();  // TODO: ??
  }
  // I: Lock coupling with mutex [SHOULD BE USED ONLY FOR LEAVES] WIP
  OptimisticPageGuard(OptimisticPageGuard& p_guard, Swip<T>& swip, bool)
      : bf(&BMC::global_bf->resolveSwip(p_guard.bf_s_lock, swip.template cast<BufferFrame>())),
        bf_s_lock(OptimisticGuard(bf->header.lock, OptimisticGuard::IF_LOCKED::SET_NULL))
  {
    assert(!(p_guard.bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT));
    assert(!p_guard.moved);
    if (bf_s_lock.latch_ptr == nullptr) {
      auto mutex = reinterpret_cast<std::mutex*>(&bf->header.lock + 1);
      mutex->lock();
      bf_s_lock = OptimisticGuard(bf->header.lock, OptimisticGuard::IF_LOCKED::CAN_NOT_BE);
      bf_s_lock.mutex_locked_upfront = true;
      assert(!(bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT));
    }
    jumpmu_registerDestructor();
    p_guard.recheck();  // TODO: ??
  }
  // I: Downgrade exclusive
  OptimisticPageGuard& operator=(ExclusivePageGuard<T>&& other)
  {
    assert(!other.moved);
    bf = other.bf;
    bf_s_lock = std::move(other.bf_s_lock);
    // -------------------------------------------------------------------------------------
    if (hasBf()) {
      bf->page.LSN++;
    }
    ExclusiveGuard::unlatch(bf_s_lock);
    // -------------------------------------------------------------------------------------
    moved = false;
    other.moved = true;
    assert((bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT) == 0);
    return *this;
  }
  // I: Downgrade shared
  OptimisticPageGuard(SharedPageGuard<T>&&) = delete;
  OptimisticPageGuard& operator=(SharedPageGuard<T>&& other)
  {
    bf = other.bf;
    bf_s_lock = std::move(other.bf_s_lock);
    // -------------------------------------------------------------------------------------
    SharedGuard::unlatch(bf_s_lock);
    // -------------------------------------------------------------------------------------
    moved = false;
    other.moved = true;
    return *this;
  }
  // -------------------------------------------------------------------------------------
  // Assignment operator
  constexpr OptimisticPageGuard& operator=(OptimisticPageGuard& other) = delete;
  constexpr OptimisticPageGuard& operator=(OptimisticPageGuard&& other)
  {
    if (!moved && bf_s_lock.mutex_locked_upfront) {
      raise(SIGTRAP);
      assert(!(bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT));
      auto mutex = reinterpret_cast<std::mutex*>(bf_s_lock.latch_ptr->ptr() + 1);
      bf_s_lock.mutex_locked_upfront = false;
      mutex->unlock();
    }
    // -------------------------------------------------------------------------------------
    bf = other.bf;
    bf_s_lock = std::move(other.bf_s_lock);
    moved = false;
    manually_checked = false;
    // -------------------------------------------------------------------------------------
    other.moved = true;
    return *this;
  }
  // -------------------------------------------------------------------------------------
  template <typename T2>
  OptimisticPageGuard<T2>& cast()
  {
    return *reinterpret_cast<OptimisticPageGuard<T2>*>(this);
  }
  // -------------------------------------------------------------------------------------
  void recheck() { bf_s_lock.recheck(); }
  void recheck_done()
  {
    manually_checked = true;
    bf_s_lock.recheck();
  }
  // -------------------------------------------------------------------------------------
  // Guard not needed anymore
  PAGE_GUARD_UTILS
  // -------------------------------------------------------------------------------------
  jumpmu_defineCustomDestructor(OptimisticPageGuard) ~OptimisticPageGuard()
  {
    if (FLAGS_mutex && !moved && bf_s_lock.mutex_locked_upfront) {
      assert(!(bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT));
      auto mutex = reinterpret_cast<std::mutex*>(bf_s_lock.latch_ptr->ptr() + 1);
      bf_s_lock.mutex_locked_upfront = false;
      mutex->unlock();
    }
    jumpmu::clearLastDestructor();

    // DEBUG_BLOCK()
    // {
    //   if (!manually_checked && !moved && jumpmu::in_jump) {
    //     raise(SIGTRAP);
    //   }
    // }
  }
};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
template <typename T>
class ExclusivePageGuard
{
 public:
  PAGE_GUARD_HEADER
 protected:
  bool keep_alive = true;  // for the case when more than one page is allocated
                           // (2nd might fail and waste the first)
  // Called by the buffer manager when allocating a new page
  ExclusivePageGuard(BufferFrame& bf, bool keep_alive)
      : bf(&bf), bf_s_lock(&bf.header.lock, bf.header.lock->load()), moved(false), keep_alive(keep_alive)
  {
    assert((bf_s_lock.local_version & LATCH_EXCLUSIVE_BIT) == LATCH_EXCLUSIVE_BIT);
    // -------------------------------------------------------------------------------------
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
 public:
  // -------------------------------------------------------------------------------------
  // I: Upgrade
  ExclusivePageGuard(OptimisticPageGuard<T>&& o_guard) : bf(o_guard.bf), bf_s_lock(std::move(o_guard.bf_s_lock)), moved(false)
  {
    ExclusiveGuard::latch(bf_s_lock);
    o_guard.moved = true;
    // -------------------------------------------------------------------------------------
    jumpmu_registerDestructor();
  }
  // -------------------------------------------------------------------------------------
  static ExclusivePageGuard allocateNewPage(DTID dt_id, bool keep_alive = true)
  {
    ensure(BMC::global_bf != nullptr);
    auto& bf = BMC::global_bf->allocatePage();
    bf.page.dt_id = dt_id;
    return ExclusivePageGuard(bf, keep_alive);
  }

  template <typename... Args>
  void init(Args&&... args)
  {
    new (bf->page.dt) T(std::forward<Args>(args)...);
  }
  // -------------------------------------------------------------------------------------
  void keepAlive() { keep_alive = true; }
  // -------------------------------------------------------------------------------------
  void reclaim()
  {
    BMC::global_bf->reclaimPage(*bf);
    moved = true;
  }
  // -------------------------------------------------------------------------------------
  jumpmu_defineCustomDestructor(ExclusivePageGuard)
      // -------------------------------------------------------------------------------------
      ~ExclusivePageGuard()
  {
    if (!moved) {
      if (!keep_alive) {
        reclaim();
      } else {
        if (hasBf()) {
          bf->page.LSN++;
        }
        ExclusiveGuard::unlatch(bf_s_lock);
      }
    }
    // -------------------------------------------------------------------------------------
    jumpmu::clearLastDestructor();
  }
  // -------------------------------------------------------------------------------------
  PAGE_GUARD_UTILS
};
// -------------------------------------------------------------------------------------
template <typename T>
class SharedPageGuard
{
 public:
  PAGE_GUARD_HEADER
  // -------------------------------------------------------------------------------------
  SharedPageGuard(OptimisticPageGuard<T>&& o_guard) : bf(o_guard.bf), bf_s_lock(std::move(o_guard.bf_s_lock)), moved(false)
  {
    SharedGuard::latch(bf_s_lock);
    // -------------------------------------------------------------------------------------
    o_guard.moved = true;
  }
  ~SharedPageGuard()
  {
    if (!moved) {
      SharedGuard::unlatch(bf_s_lock);
    }
  }
  // -------------------------------------------------------------------------------------
  PAGE_GUARD_UTILS
};
// -------------------------------------------------------------------------------------
}  // namespace buffermanager
}  // namespace leanstore
// -------------------------------------------------------------------------------------
