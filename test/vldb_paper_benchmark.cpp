
/*
 * vldb_paper_benchmark.cpp
 *
 * This file is dedicated to benchmarking BwTree for VLDB submission on 
 * optimization and enhancement of BwTree
 *
 * We add two special mixed workload:
 *   (1) 50% Read 50% Update workload, with sequential, random and zipfian 
 *       distribution
 *   (2) 10 % Scan and 90 % Read workload, with sequential, random and zipfian
 *       distribution
 * The rest of the workload is the same as those in performance test:
 *   (3) Insert-only workload to populate the tree
 *       We have two options: sequential and random
 *   (4) Read-only workload
 *       We have three options: sequential, random and zipfian
 */

#include "test_suite.h"

static void GetEmptyTree() {
  
}

int main() {
  return 0; 
}
