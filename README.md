# Distributed Data Processing Systems

This repo consists of 2 projects: MapReduce and Raft.

## Project 1: MapReduce

MapReduce is a programming model and an associated implementation for processing and generating large data sets with a parallel, distributed algorithm on a cluster. The model is composed of two functions: `Map` and `Reduce`. The `Map` function performs filtering and sorting, while the `Reduce` function performs a summary operation. This project involves implementing the MapReduce model to process data efficiently.

Please navigate to the `mr` directory for more details and implementation instructions.

## Project 2: Raft Consensus Algorithm Implementation in Go

Raft is a consensus algorithm designed to be easy to understand. It is used to manage a replicated log, ensuring that multiple servers can agree on the same series of state transitions. Raft achieves this through leader election, log replication, and safety mechanisms. This project involves implementing the Raft consensus algorithm in the Go programming language.

Paper Reference: https://raft.github.io/raft.pdf

Navigate to the `raft` directory for more details and implementation instructions.
