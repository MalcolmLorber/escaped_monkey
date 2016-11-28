#!/usr/bin/python2

import socket
import sys

import persist

# Constants
DEFAULTPORT = 60000

# Utility functions
def sendMessage(opcode, message, peernum):
    """Sends a message without blocking. May throw error on timeout"""
    if not hasattr(sendMessage, 'peers'):
        sendMessage.peers = {}
        with open('peers.txt') as f:
            for i, ip in enumerate(f.read().split('\n')):
                sendMessage.peers[i] = (ip.strip(), getport(i))
        
    # TODO: spawn new thread for this. Send tcp message to localhost on error?
    message['senderid'] = peernum
    message['opcode'] = opcode
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(sendMessage.peers[peernum])
    s.send(json.dumps(message))
    s.close()

# Helper functions
def getport(peerID):
    return DEFAULTPORT + peerID

# FSM States
def leaderElection(s):
    s.leader = None

    for peerid in s.peers if peerid > s.peerID:
            sendMessage('ELECTION', '', peerid)

    
            
    while True:
        con, address = s.accept()
        msg = json.loads(con.recv(2**16))
        
        if msg['opcode'] == 'ELECTION':
            pass
        elif msg['opcode'] == 'COORDINATOR':
            pass
        elif msg['opcode'] == 'OK':
            pass
	return "Discovery"

def discovery(s):
    return "Synchronization"

def synchronization(s):
    return "broadcast"

def broadcast(s):
    return "leader_election"

# Janky finite state machine. Could probably just return functions

states = {"leader_election": leaderElection,
          "discovery": discovery,
          "synchronization": Synchronization,
          "broadcast": broadcast}

def main():
    if len(sys.argv) != 2:
        print("usage: %s ID"%sys.argv[0])

    peerID = int(sys.argv[1])
    
    s = persist.Peer(peerID)

    # Initilize the list of peers
    if not hasattr(sendMessage, 'peers'):
        s.peers = {}
        with open('peers.txt') as f:
            for i, ip in enumerate(f.read().split('\n')):
                s.peers[i] = (ip.strip(), getport(i))
                
    s.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.sock.bind(('0.0.0.0', getport(peerID)))
    s.listen(10)
        
    state = "leader_election"
    while True:
        state = states[state](s):
