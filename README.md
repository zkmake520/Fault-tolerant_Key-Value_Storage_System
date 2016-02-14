# Fault-tolerant key-value storage system 
##There are 3 layers:   
1. Application layer: creates all members and start the each Node    
2. P2P layer: a.Gossip membership layer b.key-value storage layer
3. Emulated Network: send and push messages
<br/>
##System Schema: Gossip membership protocol + Consistent hashing
1. A key-value store supporting CRUD operations (Create, Read, Update, Delete).
2. Load-balancing (via a consistent hashing ring to hash both servers and keys).
3. Fault-tolerance up to two failures (by replicating each key three times to three successive nodes in the ring, starting from the first node at or to the clockwise of the hashed key).
4. Quorum consistency level for both reads and writes (at least two replicas).
5. Stabilization after failure (recreate three replicas after failure).
