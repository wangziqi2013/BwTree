/*
 * thread_local.h - Implements BwTree's thread local storage
 *
 * For performance reasons the BwTree's garbage collection is implemented as a
 * thread local process running on each worker thread.
 */
 
namespace wangziqi2013 {
namespace bwtree {

// This is the type we use for epoch counter
using EpochType = uint64_t;

/*
 * class GarbageNode - Representation of garbages
 */
class GarbageNode {
 public:
  // This is the epoch it is deleted
  // we need to wait for all threads to have epoch counter
  // larger than this and then reclaim
  EpochType delete_epoch;

  // Opaque pointer
  void *ptr;
};

/*
 * class BwTreeThreadLocal - The thread local class that holds per-thread
 *                           data
 */
class BwTreeThreadLocal {
// Since this is a common macro (e.g. Masstree defines it as a macro)
// we first check whether it is already available
#ifndef CACHE_LINE_SIZE
  // This will define only within this class. Later included
  // files could redefine this without a name clash
  static constexpr CACHE_LINE_SIZE = 64;
#endif
 public:


};

} // namespace bwtree
} // namespace wangziqi2013

