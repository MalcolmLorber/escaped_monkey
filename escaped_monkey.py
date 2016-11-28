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

         
def sendMsgA(addr, msg):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr)
        s.send(msg)
        s.close()
    except:
        dprint("could not send message to %s"%str(addr))
        
def sendMessage(opcode, message, peernum):
    """Sends a message without blocking. May throw error on timeout"""
    if not hasattr(sendMessage, 'peers'):
        sendMessage.peers = {}
        sendMessage.threads = []
        with open('peers.txt') as f:
            for i, ip in enumerate(f.read().split('\n')):
                sendMessage.peers[i] = (ip.strip(), getport(i))
        
    # TODO: spawn new thread for this. Send tcp message to localhost on error?
    message['senderid'] = peernum
    message['opcode'] = opcode

    t = threading.Thread(target=sendMsgA, args=(sendMessage.peers[peernum], json.dumps(message)))
    t.start()
    sendMessage.threads.append(t)    

# Helper functions
def getport(peerID):
    return DEFAULTPORT + peerID

# FSM States
def leaderElection(s):
    s.leader = None

    for peerid in s.peers:
        if peerid > s.peerID:
            sendMessage('ELECTION', {}, peerid)

    
    timeleft = 5.0
    peersleft = len(filter(lambda x: x > s.peerID, s.peers))
    while timeleft > 0.01 and peersleft != 0:
        st = time.time()
        ir, outr, er = select.select([s.sock], [], [], timeleft)
        timeleft -= time.time() - st
        for sock in ir:
            con, addr = sock.accept()
            msg = json.loads(con.recv(2**16))
            dprint("Recieved message: %s" str(msg))
            if msg['opcode'] == 'OK':
                peersleft -= 1
            elif msg['opcode'] == 'ELECTION':
                if msg['senderid'] < s.peerID:
                    sendMessage('OK', {}, msg['senderid'])
                        
    while True:
        con, address = s.sock.accept()
        msg = json.loads(con.recv(2**16))
        dprint("Recieved message: %s" str(msg))
        
        if msg['opcode'] == 'ELECTION':
            if msg['senderid']<s.peerID:
                sendMessage('OK', {}, msg['senderid'])
            
        elif msg['opcode'] == 'COORDINATOR':
            s.leader=msg['senderid']
            return "discovery"
        
        elif msg['opcode'] == 'OK':
            pass
	return "discovery"

def discovery(s):
    return "synchronization"

def synchronization(s):
    return "broadcast"

def broadcast(s):
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
    
    s = persist.Peer(peerID)

    # Initilize the list of peers
    s.peers = {}
    with open('peers.txt') as f:
        for i, ip in enumerate(f.read().split('\n')):
            s.peers[i] = (ip.strip(), getport(i))
            
    s.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.sock.bind(('0.0.0.0', getport(peerID)))
    s.sock.listen(10)
        
    state = "leader_election"
    while True:
        dprint("Going to state: %s" state)
        state = states[state](s)

if __name__ == "__main__":
    main()
