/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	short port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;

        addNewMember((char *)&(memberNode->addr.addr),memberNode->heartbeat);
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    delete memberNode;
    return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size; //  message contains the original msg , not the from address or to address
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */

void MP1Node::addNewMember(char * addr, long heartbeat){
    int id = *(int*) addr;
    short port =  *(short *)(addr+4);
    addNewMember(id,port,heartbeat);
    return;
}
void MP1Node::addNewMember(int id,int port, long heartbeat){
    MemberListEntry memberListEntry (id,port,heartbeat,par->getcurrtime());
    memberNode->memberList.push_back(memberListEntry);
    memberNode->nnb++;
    return;
}

bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    MessageHdr * msg = (MessageHdr *) data;
    // msg send to the introducer 
    if(msg->msgType == JOINREQ){
        char * addr = (char *) malloc (sizeof(memberNode->addr.addr));
        long heartbeat = 0;
        memcpy(addr,(char *)(msg+1),sizeof(memberNode->addr.addr));
        // log->LOG(createAddress(addr),"Handle JOINQUEST");
        memcpy(&heartbeat, (char *)(msg+1)+sizeof(memberNode->addr.addr)+1,sizeof(long));
        addNewMember(addr, heartbeat);
        Address * joinAddr = createAddress(addr);
        log->logNodeAdd(&memberNode->addr,joinAddr);
        free(joinAddr);
        //sendBackTheReplyMsg
        sendMsg(addr,JOINREP);
        free(addr);
    } 
    else if(msg->msgType == JOINREP){
        memberNode->inGroup = true;
        // cout<<"receive reply address:"<<*(int *)memberNode->addr.addr<<" size:"<<size<<endl;
        updateMemberList(data,size);
    }
    else{
        updateMemberList(data,size);
    }
}

void MP1Node::updateMemberList(char * msg,int size){
    int readSize = sizeof(MessageHdr);
    // if(par->getcurrtime() >100)
     // cout<<"Id:"<<*(int *)memberNode->addr.addr<<"Now size start is:"<<memberNode->memberList.size()<<endl;
    // if(size > 4+24*10){
    //     cout<<"size >10 id"<<*(int *)memberNode->addr.addr<<endl;
    // }
    while(readSize < size){
        MemberInfo * memberInfo;
        memberInfo = (MemberInfo *)(msg+readSize);
        bool newMember = true;
        MemberListEntry * entry;
        for(int i = 0; i < memberNode->memberList.size(); i++){
            entry = &(memberNode->memberList[i]);
            if(memberInfo->id == entry->id && memberInfo->port == entry->port){
                // cout<<"Member Exist in list:"<<memberInfo->id<<endl;
                newMember = false;
                break;
            } 
        }
        //if it's a new member
        if(newMember){
            // cout<<"Add new Member:"<<memberInfo->id<<" beat:"<<memberInfo->heartbeat<<endl;
            addNewMember(memberInfo->id,memberInfo->port,memberInfo->heartbeat);
            Address joinAddr;
            memcpy(joinAddr.addr,&(memberInfo->id),sizeof(int));
            memcpy(joinAddr.addr+4,&(memberInfo->port),sizeof(short));
            log->logNodeAdd(&memberNode->addr,&joinAddr);
        }
        else{
            //if its heartbeat has updated
            if(entry->heartbeat < memberInfo->heartbeat){
                entry->heartbeat = memberInfo->heartbeat;
                entry->timestamp = par->getcurrtime();//use the local time
            }
        }
        readSize += sizeof(MemberInfo);
    }
    // if(par->getcurrtime() >100)
    //  cout<<"Id:"<<*(int *)memberNode->addr.addr<<"Now size is:"<<memberNode->memberList.size()<<endl;
    return;

}

void MP1Node::sendMsg(char * addr, MsgTypes msgType){
    MessageHdr * msg;
    int liveMemberCnt = 0;
    char s[1024];
    for(int i = 0; i < memberNode->memberList.size(); i++){
        if(par->getcurrtime()-memberNode->memberList[i].timestamp > TFAIL){
            continue;
        }
        liveMemberCnt++;
    }
    // cout<<"MemberInfo size:"<<sizeof(MemberInfo)<<" MessageHdr Size:"<<sizeof(MessageHdr)<<endl;
    int size =  sizeof(MemberInfo) * liveMemberCnt + sizeof(MessageHdr);
    msg = (MessageHdr *) malloc(sizeof(char) * size);
    msg->msgType = msgType;
    int j = 0;
    for(int i = 0; i < memberNode->memberList.size(); i++){
        if(par->getcurrtime()-memberNode->memberList[i].timestamp > TFAIL){
            continue;
        }
        MemberInfo memberInfo;
        memberInfo.id = memberNode->memberList[i].id;
        // if(memberInfo.id > 10){
        //     cout<< "memberId:"<<*(int *)memberNode->addr.addr<<endl;
        // }
        memberInfo.port = memberNode->memberList[i].port;
        memberInfo.heartbeat = memberNode->memberList[i].heartbeat;
        memberInfo.timestamp =  memberNode->memberList[i].timestamp;
        memcpy((char *)(msg+1)+j*sizeof(MemberInfo),&memberInfo, sizeof(memberInfo));
        j++;
    }
    Address * toAddress = createAddress(addr);
    // // for(int i = 0; i< memberNode->memberList.size(); i++){
    // //     cout<<msg->msgType<<"ID:"<<*(int *)((char *)(msg+1)+i*sizeof(MemberInfo))<<"   ";
    // // }
    // cout<<endl;
    
    // sprintf(s,"SentMemberList_live %d",liveMemberCnt);
    // log->LOG(toAddress,s);
    emulNet->ENsend(&(memberNode->addr),toAddress,(char *)msg,size);
    free(msg);
    free(toAddress);
    return;
}   
Address * MP1Node::createAddress(char *addr){
    Address *address = new Address();
    memcpy(address->addr,addr,sizeof(address->addr));
    return address;
}
/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    memberNode->heartbeat++;
    //update the heartbeat and time stamp in own member list
    MemberListEntry * entry;
    for(int i = 0; i < memberNode->memberList.size(); i++){
        entry = &(memberNode->memberList[i]);
        if(entry->id == *(int *)memberNode->addr.addr && entry->port == *(short *)(memberNode->addr.addr+4)){
            entry->heartbeat++;

            entry->timestamp = par->getcurrtime();
            // cout<<"Add heartbeat id:"<<entry->id<<" heartbeat:"<<entry->heartbeat<<" memberNode heartbaet:"<<memberNode->heartbeat<<endl;
        }
    }
    //remove those failed members
    vector<MemberListEntry>::iterator iter;
    for(iter = memberNode->memberList.begin(); iter != memberNode->memberList.end();){
        if(par->getcurrtime()-(*iter).timestamp > TREMOVE){
            Address removeAddr;
            memcpy(removeAddr.addr,&((*iter).id),sizeof(int));
            memcpy(removeAddr.addr+4,&((*iter).port),sizeof(short));
            log->logNodeRemove(&(memberNode->addr),&removeAddr);
            iter = memberNode->memberList.erase(iter);
        }
        else{
            iter++;
        }
    }
    //randomly pick up some neighboor members to send
    double possibility = 0.3;
    for(int i = 0; i < memberNode->memberList.size(); i++){
        entry = &(memberNode->memberList[i]);
        if(entry->id != *(int *)memberNode->addr.addr ){
            if((rand()%100 * 1.0) /100 > possibility){
                char * addr = (char *)malloc(sizeof(memberNode->addr.addr));
                *(int *)addr = entry->id;
                *(short*)(addr+4) = entry->port; 
                sendMsg(addr,PING);
                free(addr);
            }
        }
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
