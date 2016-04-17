# BwTree
This is a street strength implementation of Bw-Tree, the Microsoft's implementation of which is currently deployed in SQL Server Hekaton, Azure DocumentDB, Bing and other Microsoft products.

Paper for reference
===================
http://research.microsoft.com/pubs/246926/p2067-makreshanski.pdf

Section 5.1 "Lock-Free Programming Difficulties" points out at least three scenario where we should take care when implementing Bw-Tree

http://www.msr-waypoint.com/pubs/178758/bw-tree-icde2013-final.pdf

This is the official Bw-Tree paper with many of the details blurred

http://db.disi.unitn.eu/pages/VLDBProgram/pdf/DBRank/dbrank2013-invited2.pdf

Here is again an interesting paper which has nothing new about Bw-Tree but there is a short summary of Bw-Tree in section 1.1
