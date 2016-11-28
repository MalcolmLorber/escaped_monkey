#!/usr/bin/python2

import socket
import sys

import persist

# Constants
DEFAULTPORT = 60000

# Utility functions
def sendMessage(message, peernum, peers):
    """Sends a message without blocking. May throw error on timeout"""
    # TODO: spawn new thread for this. Send tcp message to localhost on error?
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(peers[peernum])
    s.send(message)
    s.close()

# Helper functions
def getport(peerID):
    return DEFAULTPORT + peerID

# FSM States
def leaderElection(s):
    s.leader = None
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
        print("usage: %s ID"%sys.argv[0)]
        
    peerID = int(sys.argv[1])
    
    s = persist.Peer(peerID)
    s.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.sock.bind(('0.0.0.0', getport(peerID)))
    s.listen(10)
        
    state = "leader_election"
    while True:
        state = states[state](s):
