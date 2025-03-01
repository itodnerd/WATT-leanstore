#pragma once
#include "core/BTreeGeneric.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
class BTreeLL : public KVInterface, public BTreeGeneric
{
  public:
   struct WALBeforeAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALInsert : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   struct WALUpdate : WALEntry {
      u16 key_length;
      u16 delta_length;
      u8 payload[];
   };
   struct WALRemove : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   virtual OP_RESULT updateSameSizeInPlace(u8* key,
                                           u16 key_length,
                                           function<void(u8* value, u16 value_size)>,
                                           UpdateSameSizeInPlaceDescriptor&) override;
   virtual OP_RESULT remove(u8* key, u16 key_length) override;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                             function<void()>) override;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                              function<void()>) override;
   // -------------------------------------------------------------------------------------
   virtual u64 countPages() override;
   virtual u64 countEntries() override;
   virtual u64 getHeight() override;
   // -------------------------------------------------------------------------------------
   static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static void todo(void* btree_object, const u8* wal_entry_ptr, const u64 version_worker_id, const u64 tts);
   static void checkpoint(void*, BufferFrame& bf, u8* dest);
   static std::unordered_map<std::string, std::string> serialize(void* btree_object);
   static void deserialize(void* btree_object, std::unordered_map<std::string, std::string> serialized);
   static DTRegistry::DTMeta getMeta();
   // -------------------------------------------------------------------------------------
  protected:
   // WAL / CC
   static void generateDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src);
   static void applyDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src);
   static void generateXORDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src);
   static void applyXORDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src);
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
