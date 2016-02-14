/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
#define QUORUM 2
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

int totalMessage = 0;
/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	// delete memberNode;
}

/**
 *	Get index of current node in the ring table 
 * @param  ring ring table
 * @return      index
 */
int MP2Node::getIndexInRing(vector<Node> & ring){
	for(int i = 0;i < ring.size(); i++){
		if(ring[i].nodeAddress == getMemberNode()->addr){
			return i;
		}
	}
	return -1;
}
/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	//TODO:should use a variable in MP1Node to show whether the membership has been updated
	//		can save the following comparasion time
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	bool stabilization = false;
	//get all joined nodes and failed nodes
	
	for(int i =0; i < curMemList.size(); i++){
		if(i == ring.size() || !(ring[i].nodeAddress == curMemList[i].nodeAddress)){
			stabilization = true;
			break;
		}
	}
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(stabilization == true ){
		stabilizationProtocol(curMemList);
	}
	ring = curMemList;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */


void MP2Node::pushNewTransactionInfo(string key, string value,int transId, MessageType messageType){
	TransInfo transInfo;
	transInfo.positiveReplyCount = 0;
	transInfo.negativeReplyCount = 0;
	transInfo.messageType = messageType;
	transInfo.startTime = par->getcurrtime();
	transInfo.key = key;
	transInfo.value = value;
	transInfos.emplace(transId,transInfo);
	return;
}
void MP2Node::clientCreateOrUpdate(string key,string value, MessageType messageType){
	vector<Node> addrVec = findNodes(key);
	totalMessage++;
	if(addrVec.size() == 0){
		return;
	}
	int transId = g_transID++;
	int a = (getMemberNode()->addr.addr[0]);
	cout<<"TranId "<<transId<<" Node "<<a<<" Create "<<key<<" "<<value<<endl;
	for(int i = 0, replicaType = PRIMARY;i <addrVec.size(); i++,replicaType++){
		Message * message = new Message(transId,getMemberNode()->addr,messageType,key,value,static_cast<ReplicaType>(replicaType));
		emulNet->ENsend(&(memberNode->addr),&(addrVec[i].nodeAddress),message->toString());
		printf(" TO %d\n",addrVec[i].nodeAddress.addr[0]);
		delete(message);
	}
	pushNewTransactionInfo(key,value,transId,messageType);
	return;
}

void MP2Node::clientReadOrDelete(string key,MessageType messageType){
	vector<Node> addrVec = findNodes(key);
	if( addrVec.size() == 0){
		return;
	}
	int transId = g_transID++;
	cout<<"TranId "<<transId<<" Read or Delete "<<key<<endl;
	for(int i = 0;i <addrVec.size(); i++){
		Message * message = new Message(transId,getMemberNode()->addr,messageType,key);
		emulNet->ENsend(&(memberNode->addr),&(addrVec[i].nodeAddress), message->toString());
		delete(message);
	}
	pushNewTransactionInfo(key,"",transId,messageType);
	// cout<<"TranId "<<transId<<" Read 222or Delete "<<key<<endl;
	return;
}
void MP2Node::clientCreate(string key, string value) {
	clientCreateOrUpdate(key,value,CREATE);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	clientReadOrDelete(key,READ);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	clientCreateOrUpdate(key,value,UPDATE);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	clientReadOrDelete(key,DELETE);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	// Insert key, value, replicaType into the hash table
	Entry entry (value,par->getcurrtime(),replica);
	string newVal = entry.convertToString();
	return ht->create(key,newVal);

}


/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	// Read key from local hash table and return value
	Entry entry (ht->read(key));
	return entry.value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	// Update key in local hash table and return true or false
	Entry entry (value,par->getcurrtime(),replica);
	string newVal = entry.convertToString();
	return ht->update(key,newVal);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	// Delete the key from the local hash table
	cout<<"delete "<<key<<endl;
	return ht->deleteKey(key);
}



void MP2Node::replyMessageHandler(Message * msg){
	bool coordinator = true;
	int transId = msg->transID;
	if(transId != -1){
		map<int,TransInfo>::iterator it;
		it = transInfos.find(transId);
	  	if (it != transInfos.end()){
			if(msg->success){
	  			//if success replies have sum up to QUORUM, then coordinator knows the transaction is successful
	  			if(++it->second.positiveReplyCount >= QUORUM){
	  				if(msg->type == READREPLY){
						log->logReadSuccess(&(getMemberNode()->addr),coordinator,msg->transID,it->second.key,msg->value);
	  				}
	  				else if(msg->type == REPLY){
		  				switch(it->second.messageType){
		  					case CREATE:
		  						log->logCreateSuccess(&(getMemberNode()->addr),coordinator,msg->transID,it->second.key,it->second.value);
		  						break;
		  					case UPDATE:
		  						log->logUpdateSuccess(&(getMemberNode()->addr),coordinator,msg->transID,it->second.key,it->second.value);
		  						break;
		  					case DELETE:
		  						log->logDeleteSuccess(&(getMemberNode()->addr),coordinator,msg->transID,it->second.key);
		  						break;
		  					case READREPLY:
		  						break;
		  					default:
		  						break;
		  				}	
		  				transInfos.erase(it);
	  				}
	  				else{
	  					//wrong
	  				}
	  			}
	  		}
			else{
				if(++it->second.negativeReplyCount >= QUORUM){
					if(msg->type == READREPLY){
						log->logReadFail(&(getMemberNode()->addr),coordinator,msg->transID,it->second.key);
					}
					else if(msg->type == REPLY){
		  				switch(it->second.messageType){
		  					case CREATE:
		  						log->logCreateFail(&(getMemberNode()->addr),coordinator,msg->transID,it->second.key,it->second.value);
		  						break;
		  					case UPDATE:
		  						log->logUpdateFail(&(getMemberNode()->addr),coordinator,msg->transID,it->second.key,it->second.value);
		  						break;
		  					case DELETE:
		  						log->logDeleteFail(&(getMemberNode()->addr),coordinator,msg->transID,it->second.key);
		  						break;
		  					default:
		  						break;

		  				}	
		  				transInfos.erase(it);
					}
					else{
						//wrong message
					}

				}	
			}
		}
		else{
			// int a= getMemberNode()->addr.addr[0];
			// cout<<a<<" Get wrong reply message "<<msg->transID<<endl;
		}
	}
}

void MP2Node::sendReplyMessage(Message *msg, Address &addr,string val){
	Message * replyMessage = new Message(msg->transID,getMemberNode()->addr,val);
	emulNet->ENsend(&(memberNode->addr),&(addr), msg->toString());
	delete(replyMessage);
}

void MP2Node::sendReplyMessage(Message *msg,Address & addr,bool ifSuccess){
	if(msg->transID != -1){
		Message * replyMessage = new Message(msg->transID,getMemberNode()->addr,REPLY,ifSuccess);
		int a = addr.addr[0];
		int b = getMemberNode()->addr.addr[0];
		cout<<"Send reply from "<<b<<" to "<<a<<endl;
		emulNet->ENsend(&(memberNode->addr),&(addr), replyMessage->toString());
		delete(replyMessage);
	}	
	return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. 		Parts of it are already implemented
	 */
	char * data;
	int size;
	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		 // if(par->getcurrtime() == INSERT_TIME)
		// cout<<"size "<<memberNode->mp2q.size()<<endl;
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		bool ifCreateSuccess = false;
		bool ifUpdateSuccess = false;
		bool ifDeleteSuccess = false;
		string val;
		Message * msg = new Message(message);
		// cout<<"Msg "<<msg->toString()<<endl;
		// break;
		bool notCoordinator = false;
		switch(msg->type){
			case CREATE:
				ifCreateSuccess = createKeyValue(msg->key,msg->value,msg->replica);
				if(msg->transID!=-1){
					if(ifCreateSuccess){
						log->logCreateSuccess(&(getMemberNode()->addr),notCoordinator,msg->transID,msg->key,msg->value);
					}
					else{
						log->logCreateFail(&(getMemberNode()->addr),notCoordinator,msg->transID,msg->key,msg->value);	
					}
					sendReplyMessage(msg,msg->fromAddr,ifCreateSuccess);
				}
				break;
			case READ:
				val = readKey(msg->key);
				if(msg->transID!=-1){
					if(!val.empty()){
						log->logReadSuccess(&(getMemberNode()->addr),notCoordinator,msg->transID,msg->key,val);
					}
					else{
						log->logReadFail(&(getMemberNode()->addr),notCoordinator,msg->transID,msg->key);
					}
				    sendReplyMessage(msg,msg->fromAddr,val);
				}
				break;
			case UPDATE:
				ifUpdateSuccess = updateKeyValue(msg->key,msg->value,msg->replica);
				if(msg->transID!=-1){
					if(ifUpdateSuccess){
						log->logUpdateSuccess(&(getMemberNode()->addr),notCoordinator,msg->transID,msg->key,msg->value);
					}
					else{
						log->logUpdateFail(&(getMemberNode()->addr),notCoordinator,msg->transID,msg->key,msg->value);	
					}
					sendReplyMessage(msg,msg->fromAddr,ifUpdateSuccess);
				}
				break;
			case DELETE:
				ifDeleteSuccess = deletekey(msg->key);
				if(msg->transID!=-1){
					if(ifDeleteSuccess){
						log->logDeleteSuccess(&(getMemberNode()->addr),notCoordinator,msg->transID,msg->key);
					}
					else{
						log->logDeleteFail(&(getMemberNode()->addr),notCoordinator,msg->transID,msg->key);	
					}
					if(par->getcurrtime() == 102){
						int a = msg->fromAddr.addr[0];
						// cout<<"Get delete meesage from "<<a<<endl;
					}
					sendReplyMessage(msg,msg->fromAddr,ifDeleteSuccess);
				}
				break;
			case REPLY:
				replyMessageHandler(msg);
				break;
			case READREPLY:
				replyMessageHandler(msg);
				break;
			default:
				break;
		}
				

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			// TODO: improve by using binary search
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					//i+1 % size  GOOD Implemenation about corner case
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */

void MP2Node::updateLocalReplicaToPrimary(ReplicaType type){
	map<string, string>::iterator it;
	// create replicas on two successor nodes
	for(it = ht->hashTable.begin(); it != ht->hashTable.end(); it++)
	{
		Entry * entry = new Entry(it->second);
		if(entry->replica == type){
			entry->replica = PRIMARY;
			it->second = entry->convertToString();
		}
		delete(entry);
	}

}
void MP2Node::updateOrDeleteReplica(Node &node, int idx, MessageType messageType, ReplicaType type,ReplicaType replicaType){
	map<string, string>::iterator it;
	// create replicas on two successor nodes
	for(it = ht->hashTable.begin(); it != ht->hashTable.end(); it++)
	{
		Entry * entry = new Entry(it->second);
		if(entry->replica == type){
			string value = entry->value;
			string key = it->first;
			//about the transId?
			if(messageType == CREATE){
				Message message (-1,getMemberNode()->addr,messageType,key,value,replicaType);
				emulNet->ENsend(&(memberNode->addr),&(node.nodeAddress),message.toString());
			}
			else{
				Message message (-1,getMemberNode()->addr,messageType,key);
				emulNet->ENsend(&(memberNode->addr),&(node.nodeAddress),message.toString());
			}
			
		}
		delete(entry);
	}
}
void MP2Node::stabilizationProtocol( vector<Node> & newRing) {
	int a = (getMemberNode()->addr.addr)[0];
	cout<<a<<endl;
	//The whole process of stabilization is a little bit intricated
	//better spend some time to think clearly of it
	//The total idea is that current node has three types of replicas, so copy or replicate them one by one!
	//Also only need to consider copying them to its two  successors!!!
	int idx = getIndexInRing(newRing);
	int secondReplica = (idx+1) % newRing.size();
	int thirdReplica = (idx+2) % newRing.size();

	if(hasMyReplicas.size() == 0){
		//current nodes's hashMyReplicas has not been set yet
		updateOrDeleteReplica(newRing[secondReplica],idx,CREATE,PRIMARY,SECONDARY);
		updateOrDeleteReplica(newRing[thirdReplica],idx,CREATE,PRIMARY,TERTIARY);
	}
	else{
		//if new successor is not same as original ones. Delete keys in original replicas and update new replicas
		if(!(newRing[secondReplica].nodeAddress == hasMyReplicas[0].nodeAddress)){
			updateOrDeleteReplica(hasMyReplicas[0],idx,DELETE,PRIMARY,SECONDARY);
			updateOrDeleteReplica(newRing[secondReplica],idx,CREATE,PRIMARY,SECONDARY);
		}	
		if(!(newRing[thirdReplica].nodeAddress == hasMyReplicas[1].nodeAddress)){
			updateOrDeleteReplica(hasMyReplicas[1],idx,DELETE,PRIMARY,TERTIARY);
			updateOrDeleteReplica(newRing[thirdReplica],idx,CREATE,PRIMARY,TERTIARY);
		}	
	}
	// note that current node don't need to delete  replica part of its original two predecessors.
	// Why? cause the predecesscor will send delete message!
	// 
	//Check if there need to rehash part of current hashtable
	// needed if there is new node joined between current node and predecessor
	//But now we don't support new node join
	int immediatePredecessor = (idx+newRing.size()-1)% newRing.size();
	int secondPredecessor = (idx+newRing.size()-2) % newRing.size();
	if(haveReplicasOf.size() == 0){
		//do nothing
	}
	else{
		// About the immediate predecessor failed: we need to consider the copy the key-value storage on this node
		// First: the primary part of this node, need to copy to thirdreplica of current node
		// Second: the secondary part of this node, if the second immediate predecessor doesn't fail, no need to copy. Why? think clearly
		// 											otherwise, copy to second and third replica of current node
		// 
		// If predecessor didn't fail, we don't need to care about the second immediate predecessor
		// Otherwise, only need to consider the replicate the primary part of second immediate predecessor.
		if(!(newRing[immediatePredecessor].nodeAddress == haveReplicasOf[0].nodeAddress)){
			//if the original immediatePredecessor has failed
			if(newRing[secondReplica].nodeAddress == hasMyReplicas[0].nodeAddress){
				updateOrDeleteReplica(newRing[secondReplica],idx,UPDATE,SECONDARY,SECONDARY);
			}
			else{
				updateOrDeleteReplica(newRing[secondReplica],idx,UPDATE,SECONDARY,SECONDARY);
			}
			updateOrDeleteReplica(newRing[thirdReplica],idx,CREATE,SECONDARY,TERTIARY);
			updateLocalReplicaToPrimary(SECONDARY);
			if(!(newRing[secondPredecessor].nodeAddress == haveReplicasOf[1].nodeAddress)){
				//second predecessor also failed
				updateOrDeleteReplica(newRing[secondReplica],idx,CREATE,TERTIARY,SECONDARY);
				updateOrDeleteReplica(newRing[thirdReplica],idx,CREATE,TERTIARY,TERTIARY);
				updateLocalReplicaToPrimary(TERTIARY);	
			}
		}

	}
	hasMyReplicas.clear();
	haveReplicasOf.clear();
	hasMyReplicas.push_back(newRing[secondReplica]);
	hasMyReplicas.push_back(newRing[thirdReplica]);
	haveReplicasOf.push_back(newRing[immediatePredecessor]);
	haveReplicasOf.push_back(newRing[secondPredecessor]);

		
}
