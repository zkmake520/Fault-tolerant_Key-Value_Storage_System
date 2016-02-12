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

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
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
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	bool stabilization = ring.size()==curMemList.size()?false:true;
	if(!stabilization){
		for(int i = 0;i < curMemList.size(); i++){
			if(!(ring[i].nodeAddress == curMemList.nodeAddress)){
				stabilization = true;
				break;
			}
		}
		if(stabilization){
			ring = curMemList;
		}
	}
	else{
		ring = curMemList;
	}

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(stabilization == true && ring.size() > 0){
		//first 
		hasMyReplicas.clear();
		haveReplicasOf.clear();
		int i =0;
		for(i = 0;i < ring.size(); i++){
			if(ring[i].nodeAddress == getMemberNode().nodeAddress){
				break;
			}
		}
		hasMyReplicas.push_back(ring[(i+1)%RING_SIZE]);
		hasMyReplicas.push_back(ring[(i+2)%RING_SIZE]);
		haveReplicasOf.push_back(ring[(i-1)%RING_SIZE]);
		haveReplicasOf.push_back(ring[(i-2)%RING_SIZE]);
		stabilizationProtocol();
	}
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
	transInfo->positiveReplyCount = 0;
	transInfo->negativeReplyCount = 0;
	transInfo->messageType = messageType;
	transInfo->startTime = par->getcurrtime();
	transInfo->key = key;
	transInfo->value = value;
	transInfos.emplace(transId,transInfo);
	return;
}
void MP2Node::clientCreateOrUpdate(string key,string value, MessageType messageType){
	vector<Node> addrVec = findNodes(key);
	if(addrVec == NULL || addrVec.size() == 0){
		return;
	}
	Message ** messageVec = (Message *)malloc(sizeof(Message *)*addVec.size());
	int transID = 1;
	if(messageVec == NULL){
		return;
	}
	else{
		for(int i = 0, replicaType = PRIMARY;i <addrVec.size(); i++,replicaTyp++){
			Message * message = new Message(transID,getMemberNode->addr,messageType,key,value,static_cast<ReplicaType>(replicaType));
			emulNet->ENsend(&(memberNode->addr),&(addrVec[i].nodeAddress),message->toString());
		}
	}
	pushNewTransactionInfo(key,value,transId,messageType);
	return;
}

void MP2Node::clientReadOrDelete(string key,MessageType messageType){
	vector<Node> addrVec = findNodes(key);
	if(addrVec == NULL || addrVec.size() == 0){
		return;
	}
	Message ** messageVec = (Message *)malloc(sizeof(Message *)*addVec.size());
	int transId = g_transID++;
	if(messageVec == NULL){
		return;
	}
	else{
		for(int i = 0;i <addrVec.size(); i++){
			Message * message = new Message(transId,getMemberNode->addr,messageType,key);
			emulNet->ENsend(&(memberNode->addr),&(addrVec[i].nodeAddress), message->toString());
		}
	}
	pushNewTransactionInfo(key,NULL,transId,messageType);
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
	return ht->read(key);
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
	return ht->deleteKey(key);
}



void MP2Node::replyMessageHandler(Message * msg){
	bool coordinator = true;
	int transId = msg->transID;
	map<int,transInfo>::iterator it;
	it = transInfos.find(transId);
  	if (it != transInfos.end()){
		if(msg->success){
	  			//if success replies have sum up to QUORUM, then coordinator knows the transaction is successful
	  			if(++it->second.positiveReplyCount >= QUORUM){
	  				if(msg->type == READREPLY){
						log->logReadSuccess(&(getMemberNode()->addr),coordinator,msg->transId,it->second.key,msg->value);
	  				}
	  				else if(msg->type == REPLY){
		  				switch(it->second.messageType){

		  					case CREATE:
		  						log->logCreateSuccess(&(getMemberNode()->addr),coordinator,msg->transId,it->second.key,it->second.value);
		  						break;
		  					case UPDATE:
		  						log->logUpdateSuccess(&(getMemberNode()->addr),coordinator,msg->transId,it->second.key,it->second.value);
		  						break;
		  					case DELETE:
		  						log->logDeleteSuccess((&(getMemberNode()->addr),coordinator,msg->transId,it->second.key);
		  						break;
		  					case READREPLY:
		  						
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
		}
		else{
			if(++it->second.negativeReplyCount >= QUORUM){
				if(msg->type == READREPLY){
					log->logReadFail(&(getMemberNode()->addr),coordinator,msg->transId,it->second.key,msg->value);
				}
				else if(msg->type == REPLY){
	  				switch(it->second.messageType){
	  					case CREATE:
	  						log->logCreateFail(&(getMemberNode()->addr),coordinator,msg->transId,it->second.key,it->second.value);
	  						break;
	  					case UPDATE:
	  						log->logUpdateFail(&(getMemberNode()->addr),coordinator,msg->transId,it->second.key,it->second.value);
	  						break;
	  					case DELETE:
	  						log->logDeleteFail((&(getMemberNode()->addr),coordinator,msg->transId,it->second.key);
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
		cout<<"Reply Message Wrong "<<msg->transID<<endl;
	}
}

void MP2Node::sendReplyMessage(Message *msg, string val){
	Message * replyMessage = new Message(msg->transId,getMemberNode()->addr,val);
	emulNet->ENsend(&(memberNode->addr),&(addrVec[i].nodeAddress), message->toString());
	delete(replyMessage);
}

void MP2Node::sendReplyMessage(Message *msg, bool ifSuccess){
	if(msg->transId != -1){
		Message * replyMessage = new Message(msg->transId,getMemberNode()->addr,REPLY,ifSuccess);
		emulNet->ENsend(&(memberNode->addr),&(addrVec[i].nodeAddress), message->toString());
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
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		Message * msg = new Message(message);
		bool notCoordinator = false;
		switch(msg->type){
			case CREATE:
				bool ifSuccess = createKeyValue(msg->key,msg->value,msg->replica);
				if(ifSuccess){
					log->logCreateSuccess(&(getMemberNode()->addr),notCoordinator,msg->transId,msg->key,msg->value);
				}
				else{
					log->logCreateFail(&(getMemberNode()->addr),notCoordinator,msg->transId,msg->key,msg->value);	
				}
				sendReplyMessage(msg,ifSuccess);
				break;
			case READ:
				String val = read(msg->key);
				if(val == NULL){
					log->logReadSuccess(&(getMemberNode()->addr),notCoordinator,msg->transId,msg->key,val);
				}
				else{
					log->logCreateFail(&(getMemberNode()->addr),notCoordinator,msg->transId,msg->key);
				}
				sendReplyMessage(msg,val);
				break;
			case UPDATE:
				ifSucceess = updateKeyValue(msg->key,msg->value,msg->replica);
				if(ifSuccess){
					log->logCreateSuccess(&(getMemberNode()->addr),notCoordinator,msg->transId,msg->key,msg->value);
				}
				else{
					log->logCreateFail(&(getMemberNode()->addr),notCoordinator,msg->transId,msg->key,msg->value);	
				}
				sendReplyMessage(msg,ifSuccess);
				break;
			case DELETE:
				ifSuccess = deletekey(msg->key);
				if(ifSuccess){
					log->logDeleteSuccess(&(getMemberNode()->addr),notCoordinator,msg->transId,msg->key);
				}
				else{
					log->logDeleteFail(&(getMemberNode()->addr),notCoordinator,msg->transId,msg->key);	
				}
				sendReplyMessage(msg,ifSuccess);
				break;
			case REPLY:
				replyMessageHandler(msg);
				break;
			case READREPLY:
				readReplyMessageHandler(msg);
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
			// 
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
void MP2Node::stabilizationProtocol() {
		
}
