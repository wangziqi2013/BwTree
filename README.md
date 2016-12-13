# Open BwTree [![Build Status](https://travis-ci.org/wangziqi2013/BwTree.svg?branch=peloton)](https://travis-ci.org/wangziqi2013/BwTree) [![Coverage Status](https://coveralls.io/repos/github/wangziqi2013/BwTree/badge.svg?branch=master)](https://coveralls.io/github/wangziqi2013/BwTree?branch=master)
This is an implementation of BwTree, based on a design from Microsoft Research[1]. The official implementation of BwTree by Microsoft is currently deployed in SQL Server Hekaton, Azure DocumentDB, Bing and other Microsoft products.   

BwTree is a general purpose, concurrent and lock-free B+-Tree index. It allows for multiple threads modifying and/or querying the tree concurrently without corrupting the tree or giving inconsistent results. However, BwTree only guarantees atomicity of operations, and is not a concurrency control agent. If both atomicity and isolation is required, an extra concurrency control layer must be implemented. 

This project is developed together with Peloton, a self-adaptive database system prototype by Carnegie Mellon University. Though we strive to maintain BwTree as a standalong module with maximum portability and easiness to use, the special properties of lock-free data structure and of BwTree itself renders some standarized interfaces difficult or impossible to implement (for sake of atomicity), or leads to very inefficient implementations. But still, switching from a C++ STL compliant container to BwTree usually requires only few lines of code change in most common cases, and we are working to simplify things for not-so-common uses.

Benchmark 
========= 

(Since currently this repo is undergoing radical design changes, benchmark will be unavailable for few weeks)

Improvements 
================================

ABORT Delta
-----------
In the official BwTree paper, removing a node is described as posting a node remove delta on the removed node, finishing it by posting an index term delete delta on top of the parent node delta chain. This procedure has a potential problem when the parent is undergoing a split and the split key is unfortunately chosen to be pointing to the removed node. In this case, after parent node split, its left most child is logically merged into the left sibling of the parent node, which implicitly changes both the low key of the parent and the high key of its left sibling.

Our approach to avoid this problem is to post a special ABORT node on the parent before we post remove node. This ABORT node blocks all further access of the parent node, and in the meanwhile it prevents any thread that has already taken the snapshot before the ABORT node is posted posting another SMO. After remove node delta is posted, we remove this ABORT node, and jump to the left sibling to finish the remove SMO. Also when posting a node split delta, we always check whether the chosen key is mapped to a removed child. If it is the case then we do not split and continue.

Posting InnerDeleteNode
-----------------------
Besides that, when finishing a node split delta by posting index term insert node on the parent node delta chain, the paper suggests that the parent node snapshot we have taken while traversing down needs to be checked against the current most up-to-date (i.e. at least the most up-to-date parent node after we have taken the snapshot of the child split delta node), to avoid very delicate problems that would only happen under a super high insert-delete contention (as in mixed-test environment in this repo). But actually such check is unnecessary in the case of split delta (though it is a must-do for merge delta), since even if we have missed few inner data nodes on the out-of-date parent node delta chain, the worst result is observing inconsistent Key-NodeID pair inside the parent node delta chain, which is a hint that the current snapshot is quite old, and that an abort is necessary.

But on the other hand, when dealing with node merge delta, it is mandatory that we check for the status of the parent node after taking the snapshot of the merge delta node. Because for merge delta node's help-along protocol, if we go back to the parent node, and could not find the separator key that is going to be deleted, we assume that it has already been finished by other threads, and thus will proceed with the current operation that might overwrite the merge delta before it is finished. Imagine the case where the snapshot was taken before an InnerInsertNode was posted. In this case, if we use the old parent node snapshot, then a wrong conslusion that the merge delta has already been finished will be reached.

Known Problem
=============

Wait-Freedom-less
-----------------

The ABORT mechanism stated above renders BwTree non-wait-free, since it essentially locks the parent node of the node being removed, and one thread will spin on that ABORT node if it intends to post on the locked parent. Even worse, if the node posting ABORT node dies before it removes the ABORT, no remaining thread could succeed posting on the locked parent, and the data structure will become non-responsive (i.e. starvation to the extreme).

Mapping Table Size
------------------

Till now the size of the mapping table is fixed, and could not be extended in the case of an overflow. This problem could be alleviated by allocating a "large enough" mapping table that covers 99% of the cases, but no one could guarantee how large is "large enough", and when the size of the table will grow to the limit. For running benchmarks on data base systems this is fine, but for real world use cases it needs to be fixed, and I am open to any suggestion related to this issue.

Non-Scalable Randomness
-----------------------

Random number generator was incorrect, in a sense that the default generator provided by C++11 is a bottleneck under multicore workload, with a stable throughput of 1.00 M op/second in our configuration. This has already been corrected recently, but we are still investigating into this issue trying to find out a better solution than the current patch.

Coverity
--------

Another small issue is that coveralls data is not accurate, in a sense that lines inside header files are not detected at all. This should be attributed to gcov rather than BwTree

Compile and Run
===============
| Command | Description |
|---------|-------------|
|make prepare | Prepares build directory. Must be used first to set up the environment after each fresh clone|
|make | Compiles and links, with debugging flag, and with lower optimization level (-O2)|
|make full-speed | Compiles without debugging flag. Better performance (-O3)|
|make small-size | Compiles without debugging flag, but also optimized for smaller code size (-Os)|
|make benchmark-bwtree | Compiles and runs benchmark (seq. and random) for bwtree|
|make benchmark-all | This compiles and runs benchmark for bwtree, std::map, std::unordered\_map, stx::btree and stx::bwtree\_multimap|
|make stress-test | Runs stress test on BwTree|
|make test        | Runs insert-read-delete test for multiple times with different patterns|
|make epoch-test  | Runs epoch manager test|
|make infinite-insert-test | Runs random insert test on a random pattern|
|make email-test | Runs email test. This requires a special email input file that we will not provide for some reason|
|make mixed-test | Runs insert-delete extremely high contention test. This test is the one that fails most implementations|
|make benchmark-btree-full | Run the same benchmark as those in 'benchmark-bwtree-full' for stx::btree\_multimap|
| make benchmark-bwtree-full | Runs insert-seq read-rand read-zipf read workload for BwTree on 30 Milltion keys. Use THREAD\_NUM=xxx before make command to specify the number of threads used for testing |

Releases
========
You could either use git checkout to view these releases, or download them through Github web interface.

| Release Name | Description |
|--------------|-------------|
| baseline | A stable working version of BwTree used as testing baseline against design changes |
| fixed-forward-iterator | Preallocation + fixed forward iterator |
| inner-search-hint | Search hint inside InnerNode traversal |

Misc 
====

Under stl\_test folder there are C++ source files illustrating how STL and our private container libraries could be used, as well as some testing / benchmarking / demo code. Using make command to compile and link these STL test cases.

For email_test, a text file with email addresses generated in some way is required, which is not provided here for some reason. However, generating your own email file should not be difficult.

References
==========
[1] Levandoski, Justin J., David B. Lomet, and Sudipta Sengupta. "The Bw-Tree: A B-tree for new hardware platforms." In Data Engineering (ICDE), 2013 IEEE 29th International Conference on, pp. 302-313. IEEE, 2013. 

License
=======
Personally I would like to open source it without any license. But in order to stay complianct with Peloton's licene, the  licence for OpenBwTree is Apache License (https://www.apache.org/licenses/LICENSE-2.0)
