# BwTree
This is a street strength implementation of Bw-Tree, the Microsoft's implementation of which is currently deployed in SQL Server Hekaton, Azure DocumentDB, Bing and other Microsoft products.

![Cover Image](https://raw.githubusercontent.com/wangziqi2013/BwTree/master/cover.png)

![Cover Image](https://raw.githubusercontent.com/wangziqi2013/BwTree/peloton/result-2016-06-17.jpg)


Paper for reference
===================
http://research.microsoft.com/pubs/246926/p2067-makreshanski.pdf

Section 5.1 "Lock-Free Programming Difficulties" points out at least three scenario where we should take care when implementing Bw-Tree

http://www.msr-waypoint.com/pubs/178758/bw-tree-icde2013-final.pdf

This is the official Bw-Tree paper with many of the details blurred

http://db.disi.unitn.eu/pages/VLDBProgram/pdf/DBRank/dbrank2013-invited2.pdf

Here is again an interesting paper which has nothing new about Bw-Tree but there is a short summary of Bw-Tree in section 1.1

Potential Problem with the Paper
================================
In the official paper of Bw-Tree, removing a node is described as posting a node remove delta on the removed node first, and then we make progress with this SMO using the help-along protocol, finishing it by posting an index term delete delta on top of its parent. This procedure has a potential problem when the parent is undergoing a split, and the split key is unfortunately chosen to be pointing to the removed node. In this case, after parent node split, its left most child is logically merged into the left sibling of the parent node, which implicitly changes both the low key of the parent and the high key of its left sibling.

Our approach to avoid this problem is to post a special ABORT node on the parent before we post remove node. This ABORT node blocks all further access of the parent node, and in the meanwhile it prevents any thread that has already taken the snapshot before it is posted posting another SMO. After remove node delta is posted, we remove this ABORT node, and jump to the left sibling to finish the remove SMO. Also when posting a node split delta, we always check whether the chosen key is mapped to a removed child. If it is the case then we do not split and continue.

Compile and Run
===============
This Bw-Tree implementation heavily makes use of template, but is flexible enough for those data types that have a default comparator and hasher. In order to test, you will have to include bwtree.h into your source file, and instancicate a Bw-Tree with appropriate template arguments. After that it is ready to use.

