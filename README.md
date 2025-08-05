# SegFlow: High-Performance Routing in Payment Channel Networks via Network Partitioning

This repository provides the source code for **SegFlow**, which includes both the simulation and testbed implementation.

## Contents

- **Simulation**  
  Evaluate partitioning and routing performance using the following scripts:

  - [`/simulation/partition_test.py`](./simulation/partition_test.py) – Tests network partitioning performance  
  - [`/simulation/routing_test.py`](./simulation/routing_test.py) – Tests routing efficiency and effectiveness

- **Testbed**  
  The testbed extends the implementation from Flash [[1]](#reference), with SegFlow implemented in:

  - [`/testbed/src/server/main.go`](./testbed/src/server/main.go)

## Reference

[1] P. Wang, H. Xu, X. Jin, and T. Wang, “Flash: Efficient dynamic routing for offchain networks,” in *Proc. 15th Int. Conf. Emerg. Netw. Exp. Technol. (CoNEXT)*, 2019, pp. 370–381.
