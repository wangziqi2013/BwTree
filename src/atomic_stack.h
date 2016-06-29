
#include <atomic>
#include <cassert>

/*
 * class AtomicStack - Thread-safe lock-free stack implementation
 *
 * DO NOT USE THIS CLASS BEFORE YOU HAVE READ THE FOLLOWING:
 *
 * 1. This implementation uses a fixed sized array as stack base.
 *    Please make it sufficiently large in the situation where data
 *    item count could be upper bounded. If not please use a linked-list
 *    based one
 * 2. This implementation does not check for any out of bound access under
 *    release mode (in debug mode it checks). Please be careful and again
 *    make sure the actual size of the stack could be upper bounded
 * 3. Type T is required to be trivially copy assign-able and trivially
 *    constructable. The first is required to copy the element
 *    into the stack, while the second one is used to return an empty value
 *    if the stack is empty
 * 4. Currently the stack implementation only supports many consumer single
 *    producer paradigm, which fits into BwTree's NodeID recycling mechanism
 *    (i.e. one epoch thread freeing deleted NodeID and many worker threads
 *    asking for unused NodeID). In the future it is unlinkly that we would
 *    add MultiThreadPush() or something like that
 */
template <typename T, size_t STACK_SIZE>
class AtomicStack {
 private:
  // This holds actual data
  T data[STACK_SIZE];
  
  // The pointer for accessing the top of the stack
  // The invariant is that before and after any atomic push()
  // and pop() calls the top pointer always points to the
  // latest valid element in the stack
  std::atomic<T *> top_p;
  
  // This is used to buffer Push() requests in a single threaded
  // environment
  std::vector<T> buffer;
  
  /*
     * PreparePush() - Switch the stack top pointer to nullptr
     *
     * This eliminates contention between Push() and Pop(). But it does not
     * eliminate possible contention between multithreaded Push()
     *
     * This function could be called in multi-threaded environment
     *
     * The return value is the stack top before switching it to nullptr
     */
    inline T *PreparePush() {
      bool cas_ret;

      // If CAS fails this value would be automatically reloaded
      T *snapshot_top_p = top_p.load();

      // Switch the current stack to empty mode by CAS top pointer
      // to nullptr
      // This eliminates contention between Push() and Pop()
      do {
        #ifdef BWTREE_DEBUG
        assert((snapshot_top_p - data + 1) < STACK_SIZE);
        #endif

        // This contends with Pop()
        cas_ret = top_p.compare_exchange_strong(snapshot_top_p,
                                                nullptr);
      } while(cas_ret == false);

      return snapshot_top_p;
    }
    
 public:
   
    /*
     * Default Constructor
     *
     * The item pointer is initialized to point to the item before the first item
     * in the stack. This is quite dangerous if the implementation is buggy since
     * it corrupts other data structures.
     */
    AtomicStack() :
     top_p{((T *)data) - 1}
    {}
    
    /*
     * SingleThreadBufferPush() - Do not directly push the item but keep it
     *                            inside the internal buffer
     *
     * This is used as an optimization for batch push, because the overhead
     * for a single push is rather large (doing 1 CAS and 1 atomic assignment)
     * so we might want to buffer pushes first, and then commit them together
     *
     * This function could only be called in Single Threaded environment
     */
    inline void SingleThreadBufferPush(const T &item) {
      buffer.push_back(item);
      
      return;
    }

    /*
     * SingleThreadPush() - Push an element into the stack
     *
     * This function calls PreparePush() to switch stack top pointer to nullptr
     * first before writing the new value. This is to avoid advancing the
     * stack pointer without writing the actual value, and then another
     * Pop() thread comes and pops an invalid value.
     *
     * This function must be called in single-threaded environment
     */
    inline void SingleThreadPush(const T &item) {
      T *snapshot_top_p = PreparePush();
      
      // Writing in the pushed value without worrying about concurrent Pop()
      // since all Pop() would fail and return as if the stack were empty
      *(++snapshot_top_p) = item;
      
      // After this point Pop() would work
      top_p.store(snapshot_top_p);
      
      return;
    }
    
    /*
     * SingleThreadCommitPush() - Commit all buffered items
     *
     * This function does a batch commit using only 1 CAS and 1 atomic store
     *
     * This function must be called under multi-threaded environment
     */
    inline void SingleThreadCommitPush() {
      T *snapshot_top_p = PreparePush();
      
      for(auto &item : buffer) {
        *(++snapshot_top_p) = item;
      }
      
      top_p = snapshot_top_p;
      buffer.clear();
      
      return;
    }
   
   /*
    * Pop() - Pops one item from the stack
    *
    * NOTE: The reutrn value is a pair, the first element being a boolean to
    * indicate whether the stack is empty, and the second element is a type T
    * which is the value fetched from the stack if the first element is true
    *
    * NOTE 2: Pop() operation is parallel. If there is a contention with Push()
    * then Pop() will return false positive on detecting whether the stack is
    * empty, even if there are elements in the stack
    *
    * If the return value is pair{false, ... } then value T is default
    * constructed
    */
    inline std::pair<bool, T> Pop() {
      #ifdef BWTREE_DEBUG
      assert((snapshot_top_p - data) < STACK_SIZE);
      #endif
     
      while(1) {
        // Load current top pointer and check whether it points to a valid slot
        // If not then just return false. But the item might be modified
        T *snapshot_top_p = top_p.load();
        
        // This should be true if
        // 1. snapshot_top_p is data - 1 (stack is empty)
        // 2. snapshot_top_p is nullpre (there is Pop() going on)
        if(snapshot_top_p == nullptr) {
          //continue;
          // Avoid spinning here, and just return as an empty stack
          return {false, T{}};
        }
        
        if(snapshot_top_p < data) {
          // We need to trivially construct a type T element as a place holder
          return {false, T{}};
        }

        // First construct a return value
        auto ret = std::make_pair(true, *snapshot_top_p);

        // Then update the top pointer
        bool cas_ret = top_p.compare_exchange_strong(snapshot_top_p,
                                                     snapshot_top_p - 1);

        // If CAS succeeds which indicates the pointer has not changed
        // since we took its snapshot, then just return
        // Otherwise, need to retry until success or the stack becomes empty
        if(cas_ret == true) {
          return ret;
        }
      }
     
     assert(false);
     return {false, T{}};
   }
};
