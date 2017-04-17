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
  
  /*
   * Constructor
   */
  GarbageNode(EpochType p_delete_epoch, void *p_ptr) :
    delete_epoch{p_delete_epoch},
    ptr{p_ptr}
  {}
  
  /*
   * Constructor
   */
  GarbageNode() {}
  
  // We do not allow any form of copy construcion and assignment
  GarbageNode(const GarbageNode &) = delete;
  GarbageNode(GarbageNode &&) = delete;
  GarbageNode &operator=(const GarbageNode &) = delete;
  GarbageNode &operator=(GarbageNode &&) = delete;
};

/*
 * class GarbageGroup - A group of garbage nodes that are maintained in batch
 *
 * Also garbage nodes are allocated using slab allocator here, which reduces
 * the number of allocator calls when we add and free garbages
 *
 * The garbage group instance is only processed if it is full and all nodes
 * inside of it could be freed. To achieve this everytime we add a new node
 * into the group we update the epoch counter also inside the object.
 */
class GarbageGroup {
 public:
  // There are 1024 slots in each garbage group before it is full
  static constexpr GROUP_SIZE = 1024;
  
  // Number of nodes in this object
  // We use this as a next index into the array
  // and also use it to access the last deleted epoch
  int node_count;
  
  // This is the next group object in the linked list
  GarbageGroup *next_p;
  
  // This is the static array of garbage nodes. We only push
  // the latest garbage node into this list
  GarbageNode garbage_node_list[GROUP_SIZE];
  
  /*
   * Constructor
   */
  GarbageGroup() :
    node_count{0},
    next_p{nullptr},
    garbage_node_list{}
  {}
  
  // Do not allow any othre form of assignmemt and construction
  GarbageGroup(const GarbageGroup &) = delete;
  GarbageGroup(GarbageGroup &&) = delete;
  GarbageGroup &operator=(const GarbageGroup &) = delete;
  GarbageGroup &operator=(GarbageGroup &&) = delete;
  
  /*
   * IsFull() - Whether the group is full
   */
  inline bool IsFull() const {
    assert(node_count >= 0 && node_count <= GROUP_SIZE);
    
    return node_count == GROUP_SIZE;
  }
  
  /*
   * IsEmpty() - Whether the object is empty
   *
   * Note that we should never see empty group because when creating one
   * we always add at least one garnage node into it
   */
  inline bool IsEmpty() const {
    return node_count == 0;
  }
  
  /*
   * AddGarbageNode() - Adds a new garbage node into the current group
   *
   * This group must be not full otherwise assertion fails
   */
  inline void AddGarbageNode(EpochType delete_epoch, void *ptr) {
    // The group must not be full when adding new nodes
    assert(IsFull() == false);

    garbage_node_list[node_count].delete_epoch = delete_epoch;
    garbage_node_list[node_count].ptr = ptr;
    
    node_count++;
    
    return;
  }
  
  /*
   * GetLatestDeleteEpoch() - Returns the delete epoch of the last node
   *
   * We use this epoch to decide whether to GC this group or not
   */
  inline EpochType GetLatestDeleteEpoch() const {
    // Empty group does not have defined epoch and we do not
    // allow empty garbage group
    asserr(IsEmpty() == false);
    assert(node_count > 0 && node_count <= GROUP_SIZE);
    
    // Use the last garbage node
    // Note that the last garbage node's index is (node_count - 1)
    return garbage_node_list[node_count - 1].delete_epoch;
  }
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

