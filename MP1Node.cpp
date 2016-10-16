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

static int random_member(int size);

static int random_member(int size) { 
    srand(time(NULL));
    int r = rand()%(size);
    return r;
}

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
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    MemberListEntry m(id, port, memberNode->heartbeat, par->getcurrtime());
	memberNode->memberList.push_back(m);

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
    return 0;
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
    	size = memberNode->mp1q.front().size;
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
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */

	MessageHdr *msg = (MessageHdr *)data;
	Address *addr = (Address *) (msg+1);

    static char s[1024];

	switch(msg->msgType) {
	    case JOINREQ:
	        sprintf(s, "Received JOINREQ msg from %s", addr->getAddress().c_str());
	        log->LOG(&memberNode->addr, s);
	        join_reply(addr);
	        update(addr, data, size);
	       break;
        case JOINREP:
	        sprintf(s, "Received JOINREP msg from %s", addr->getAddress().c_str());
            log->LOG(&memberNode->addr, s);
            memberNode->inGroup = 1; // joined the group
            update(addr, data, size);
            break;
        case PING:
	        sprintf(s, "Received PING msg from %s", addr->getAddress().c_str());
            log->LOG(&memberNode->addr, s);
            update(addr, data, size);
            break;
        default: log->LOG(&memberNode->addr, "Received other msg");
	}

	 return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    // remove timed out/suspects
    cleanupMemberList();

	updateMemberList(&memberNode->addr, ++memberNode->heartbeat);

	gossip();

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

/* new methods */
void MP1Node::join_reply(Address *addr) {
    // create MSG
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + 1;
    MessageHdr * msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    msg->msgType = JOINREP;
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

	emulNet->ENsend(&memberNode->addr, addr, (char *)msg, msgsize);

}

void MP1Node::update(Address *addr, char *data, int size) {

    long heartbeat = * (long*) (data + sizeof(MessageHdr) + sizeof(Address) + 1);

    updateMemberList(addr, heartbeat);

}

/**
 * update member in memberList
 */
bool MP1Node::updateMemberList(Address *addr, long heartbeat) {
    bool updated = true;

    // get sender addr id and port to search in memberlist
    int id = *((int*)addr->addr);
    short port = *((short*)&(addr->addr[4]));

    // helper addr comparator
    struct addr_comp {
        int _id; short _port;
        addr_comp(int id, short port): _id(id), _port(port) {}
        bool operator()(MemberListEntry m) {
            return (m.id == _id) && (m.port == _port);
        }
    };

    // find sender
    vector<MemberListEntry>::iterator it;
    it = find_if(memberNode->memberList.begin(), memberNode->memberList.end(), addr_comp(id, port));

    // not found, add it
    if(it == memberNode->memberList.end()) {
        MemberListEntry m(id, port, heartbeat, par->getcurrtime());
		memberNode->memberList.push_back(m);
		log->logNodeAdd(&memberNode->addr, addr);
        return updated;
    }
    
    // found, check heartbeat value
	if (heartbeat > it->getheartbeat()) {
	    it->setheartbeat(heartbeat);
	    it->settimestamp(par->getcurrtime());
        return updated;
    }
    
    return !updated;
}

/**
 * send memberlist to random member
 */
void MP1Node::gossip(void) {

    int ml_size = memberNode->memberList.size();
    int r = random_member(ml_size);
    MemberListEntry mle = memberNode->memberList.at(r);
    Address to_addr = mle_addr(&mle);

    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + 1;
    MessageHdr * msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = PING;

	for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
	    it != memberNode->memberList.end(); it++) {

        Address addr = mle_addr(&(*it));

        memcpy((char *)(msg+1), &addr, sizeof(Address));
        memcpy((char *)(msg+1) + 1 + sizeof(Address), &it->heartbeat, sizeof(long));

    	emulNet->ENsend(&memberNode->addr, &to_addr, (char *)msg, msgsize);
    }
}

/**
 * remove members that timed out
 */
void MP1Node::cleanupMemberList() {

	int timeout = 2;

	for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
	    it != memberNode->memberList.end(); /*it++*/) {
	    Address addr = mle_addr(&(*it));
        if (memberNode->addr == addr)
            it++;
        else if (par->getcurrtime() - it->timestamp > timeout) {
			it = memberNode->memberList.erase(it);
			log->logNodeRemove(&memberNode->addr, &addr);
		} else
            it++;
	}

}

Address MP1Node::mle_addr(MemberListEntry* mle) {
		Address a;
		memcpy(a.addr, &mle->id, sizeof(int));
		memcpy(&a.addr[4], &mle->port, sizeof(short));
		return a;
}
