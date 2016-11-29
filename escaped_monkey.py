#!/usr/bin/python2

import socket
import sys
import select
import threading
import json
import time

import persist

# Constants
DEFAULTPORT = 60000

# Utility functions
def dprint(s):
    if hasattr(dprint, 'number'):
        sys.stderr.write("%02d: %s"%(dprint.number, s) + '\n')
    else:
        sys.stderr.write(str(s)+'\n')

         
def sendMsgA(addr, msg, peerID):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr)
        s.send(msg)
        s.close()
        sendMessage.peerStatus[peerID] = True
    except:
        sendMessage.peerStatus[peerID] = False
        dprint("could not send message to %s"%str(addr))
        
def sendMessage(s, opcode, message, peernum):
    """Sends a message without blocking. May throw error on timeout"""
    if not hasattr(sendMessage, 'peers'):
        sendMessage.peers = {}
        sendMessage.threads = []
        with open('peers.txt') as f:
            for i, ip in enumerate(filter(lambda x: x != '', f.read().split('\n'))):
                sendMessage.peerStatus[i] = True
                sendMessage.peers[i+1] = (ip.strip(), getport(i+1))
    
    message['senderid'] = s.peerID
    message['opcode'] = opcode

    dprint("Sending message: %s to %s"% (str(message), str(peernum)))
    t = threading.Thread(target=sendMsgA, args=(sendMessage.peers[peernum], json.dumps(message)))
    t.start()
    sendMessage.threads.append(t)

def timeloop(socket, t):
    timeleft = t
    while timeleft > 0.01:
        st = time.time()
        ir, outr, er = select.select([socket], [], [], timeleft)
        timeleft -= time.time() - st
        for sock in ir:
            con, addr = sock.accept()
            msg = json.loads(con.recv(2**16))
            dprint("Recieved message: %s"%str(msg))
            yield msg


# Helper functions
def getport(peerID):
    return DEFAULTPORT + peerID

# FSM States
def leaderElection(s):
    s.leader = None
        
    for peerid in s.peers:
        if peerid > s.peerID:
            sendMessage(s, 'ELECTION', {}, peerid)

    peersleft = len(filter(lambda x: x > s.peerID, s.peers))
    #if peersleft != 0:
    for msg in timeloop(s.sock, 2.0):
        if msg['opcode'] == 'OK':
            peersleft -= 1
            if peersleft == 0:
                break
        elif msg['opcode'] == 'ELECTION':
            dprint("Recieved ELECTION from %s, adding to connected"%msg['senderid'])
            if msg['senderid'] < s.peerID:
                sendMessage(s, 'OK', {}, msg['senderid'])
        elif msg['opcode'] == 'COORDINATOR':
            s.leader = msg['senderid']
            return "discovery"  
        else:
            dprint("extranious message")
                    
    if peersleft == len(filter(lambda x: x > s.peerID, s.peers)):
        dprint("I AM LEADER. MWAHAHAHAHAHA")
        s.leader = s.peerID
        for peerid in filter(lambda x: x < s.peerID, s.peers):
            sendMessage(s, 'COORDINATOR', {}, peerid)
        return "discovery"

    dprint("waiting for message from our great leader")
    
    for msg in timeloop(s.sock, 2.0):
        if msg['opcode'] == 'COORDINATOR':
            s.leader = msg['senderid']
            return "discovery"
        elif msg['opcode'] == 'ELECTION':
            if msg['senderid'] < s.peerID:
                sendMessage(s, 'OK', {}, msg['senderid'])
        else:
            dprint("extranious message")
            
    return "leader_election"

    
def discovery_follower(s):
    sendMessage(s, 'FOLLOWERINFO', {'acceptedEpoch': s.acceptedEpoch}, s.leader)
    for msg in timeloop(s.sock, 3.0):
        if msg['opcode'] == 'NEWEPOCH' and msg['senderid'] == s.leader:
            if msg['eprime'] > s.acceptedEpoch:
                s.acceptedEpoch = msg['eprime']
                sendMessage(s, 'ACKEPOCH', {'currentEpoch': s.currentEpoch,
                                            'history': s.history,
                                            'lastZxid': s.lastZxid}, s.leader)
                return 'synchronization'
            if msg['eprime'] < s.acceptedEpoch:
                return 'leader_election'
        elif msg['opcode'] == 'ELECTION':
            sendMessage(s, 'OK', {}, msg['senderid'])
            return "leader_election"
        elif msg['opcode'] == 'COORDINATOR':
            s.leader = msg['senderid']
            return "discovery"
    
    return 'leader_election'

def discovery_leader(s):
    peersleft = len(s.peers) - 1
    epochnumbers = []
    quorum = {}
    for msg in timeloop(s.sock, 2.0):
        if msg['opcode'] == 'FOLLOWERINFO':
            epochnumbers.append(msg['acceptedEpoch'])
            quorum[msg['senderid']] = {}
            peersleft -= 1
            if peersleft == 0:
                break
            
        elif msg['opcode'] == 'ELECTION':
            sendMessage(s, 'OK', {}, msg['senderid'])
            return "leader_election"
        elif msg['opcode'] == 'COORDINATOR':
            s.leader = msg['senderid']
            return 'discovery'

    connectedPeers = filter(lamba x: sendMessage.peerStatus[x], sendMessage.peerStatus)
    if peersleft >= len(connectedPeers)/2.0:
        dprint("Failed to achive quorum. Peersleft: %d"%(peersleft))
        return 'leader_election'

    eprime = max(epochnumbers) + 1
    for i in quorum:
        sendMessage(s, 'NEWEPOCH', {'eprime': eprime}, i)

    peersleft = len(quorum)
    for msg in timeloop(s.sock, 2.0):
        if msg['opcode'] == 'ACKEPOCH':
            if msg['senderid'] in quorum:
                quorum[msg['senderid']]['currentEpoch'] = msg['currentEpoch']
                quorum[msg['senderid']]['lastZxid'] = msg['lastZxid']
                quorum[msg['senderid']]['history'] = msg['history']
                peersleft -= 1

    if peersleft != 0:
        return 'leader_election'

    # TODO: choose f
    return 'synchronization'

        
def discovery(s):
    if s.peerID == s.leader:
        return discovery_leader(s)
    else:
        return discovery_follower(s)
            
    while True:
        con, address = s.sock.accept()
        msg = json.loads(con.recv(2**16))
        dprint("Recieved message: %s"%str(msg))

        
    return "synchronization"

def synchronization(s):
    while True:
        con, address = s.sock.accept()
        msg = json.loads(con.recv(2**16))
        dprint("Recieved message: %s"%str(msg))

        if msg['opcode'] == 'ELECTION':
            sendMessage(s, 'OK', {}, msg['senderid'])
            return "leader_election"
        elif msg['opcode'] == 'COORDINATOR':
            s.leader = msg['senderid']
            return 'discovery'
    
    return "broadcast"

def broadcast(s):
    while True:
        con, address = s.sock.accept()
        msg = json.loads(con.recv(2**16))
        dprint("Recieved message: %s"%str(msg))

        if msg['opcode'] == 'ELECTION':
            sendMessage(s, 'OK', {}, msg['senderid'])
            return "leader_election"
        elif msg['opcode'] == 'COORDINATOR':
            s.leader = msg['senderid']
            return 'discovery'
    
    return "leader_election"

# Janky finite state machine. Could probably just return functions

states = {"leader_election": leaderElection,
          "discovery": discovery,
          "synchronization":  synchronization,
          "broadcast": broadcast}

def main():
    if len(sys.argv) != 2:
        dprint("usage: %s ID"%sys.argv[0])

    peerID = int(sys.argv[1])
    dprint.number = peerID
    
    s = persist.Peer(peerID)

    # Initilize the list of peers
    s.peers = {}
    with open('peers.txt') as f:
        for i, ip in enumerate(filter(lambda x: x != '', f.read().split('\n'))):
            s.peers[i+1] = (ip.strip(), getport(i+1))
            
    s.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.sock.bind(('0.0.0.0', getport(peerID)))
    s.sock.listen(10)
        
    state = "leader_election"
    while True:
        dprint("Going to state: %s"%state)
        state = states[state](s)

if __name__ == "__main__":
    main()
