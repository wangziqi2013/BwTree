# BwTree [![Build Status](https://travis-ci.org/wangziqi2013/BwTree.svg?branch=peloton)](https://travis-ci.org/wangziqi2013/BwTree)
This is a street strength implementation of Bw-Tree, the Microsoft's implementation of which is currently deployed in SQL Server Hekaton, Azure DocumentDB, Bing and other Microsoft products.

Benchmark
=========

1 Million Key; 1 thread inserting; 1 thread reading for 10 times; 8 thread reading for 10 times

![Result](https://raw.githubusercontent.com/wangziqi2013/BwTree/peloton/result/result-2016-06-17.jpg)

30 Million Key; 1 thread inserting; 1 thread reading for 10 times; 8 thread reading for 10 times

![Result](https://raw.githubusercontent.com/wangziqi2013/BwTree/peloton/result/result-2016-06-18.jpg)

Same as above, but with Bloom Filter

![Result](https://raw.githubusercontent.com/wangziqi2013/BwTree/peloton/result/result-2016-06-19.jpg)

After removing std::vector from the traversal stack

![Result](https://raw.githubusercontent.com/wangziqi2013/BwTree/peloton/result/result-2016-06-22.jpg)

Stress test with key space = 60M (expected tree size = 30M); random insert/delete; 8 worker threads

![Result](https://raw.githubusercontent.com/wangziqi2013/BwTree/peloton/result/stress-test-result-20160629.jpg)

References
===================
http://research.microsoft.com/pubs/246926/p2067-makreshanski.pdf

Section 5.1 "Lock-Free Programming Difficulties" points out at least three scenario where we should take care when implementing Bw-Tree

http://www.msr-waypoint.com/pubs/178758/bw-tree-icde2013-final.pdf

This is the official Bw-Tree paper with many of the details blurred

http://db.disi.unitn.eu/pages/VLDBProgram/pdf/DBRank/dbrank2013-invited2.pdf

Here is again an interesting paper which has nothing new about Bw-Tree but there is a short summary of Bw-Tree in section 1.1

Improvements
================================
In the official paper of Bw-Tree, removing a node is described as posting a node remove delta on the removed node first, and then we make progress with this SMO using the help-along protocol, finishing it by posting an index term delete delta on top of its parent. This procedure has a potential problem when the parent is undergoing a split, and the split key is unfortunately chosen to be pointing to the removed node. In this case, after parent node split, its left most child is logically merged into the left sibling of the parent node, which implicitly changes both the low key of the parent and the high key of its left sibling.

Our approach to avoid this problem is to post a special ABORT node on the parent before we post remove node. This ABORT node blocks all further access of the parent node, and in the meanwhile it prevents any thread that has already taken the snapshot before it is posted posting another SMO. After remove node delta is posted, we remove this ABORT node, and jump to the left sibling to finish the remove SMO. Also when posting a node split delta, we always check whether the chosen key is mapped to a removed child. If it is the case then we do not split and continue.

Compile and Run
===============
make prepare -> This prepares build directory

make         -> This only compiles and links

make benchmark-bwtree -> This compiles and runs benchmark for bwtree only

make benchmark-all    -> This compiles and runs benchmark for bwtree, std::map, std::unordered_map and std::btree

make stress-test      -> Runs stress test on BwTree until NodeID runs out and assertion fails

make test             -> Runs insert-read-delete test for multiple times with different patterns

make epoch-test       -> Runs epoch manager test

Misc
====

Under stl\_test folder there are C++ source files illustrating how STL and our private container libraries could be used, as well as some testing / benchmarking / demo code.
