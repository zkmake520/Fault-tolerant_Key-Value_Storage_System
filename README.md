# Fault-tolerant key-value storage system 
## System schema: Gossip membership protocol + Consistent hashing
1. Application layer: creates all members and start the each Node    
2. P2P layer: a.Gossip membership layer b.key-value storage layer
3. Emulated Network: send and push messages
* A key-value store supporting CRUD operations (Create, Read, Update, Delete).
* Load-balancing (via a consistent hashing ring to hash both servers and keys).
* Fault-tolerance up to two failures (by replicating each key three times to three successive nodes in the ring, starting from the first node at or to the clockwise of the hashed key).
* Quorum consistency level for both reads and writes (at least two replicas).
* Stabilization after failure (recreate three replicas after failure).


## TEST
Output will be print to dbg.log file in same directory. And Can use KVStoreGrader.sh to test the whole output
