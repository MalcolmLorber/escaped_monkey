#!/usr/bin/python

import socket
import json
import os
import sys
import time
import threading

# Constants
DEFAULTPORT = 60000
if os.path.isfile("port"):
    with open("port", 'r') as f:
        DEFAULTPORT = int(f.read().strip())


# Utility functions
def dprint(s):
    if hasattr(dprint, 'number'):
        sys.stderr.write("%02d: %s"%(dprint.number, s) + '\n')
    else:
        sys.stderr.write(str(s)+'\n')

# Helper functions
def getport(peerID):
    return DEFAULTPORT + peerID


def sendMsgA(addr, msg, peerID):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr)
        s.send(msg)
        print(s.recv(1024))
        s.close()
    except:
        dprint("could not send message to %s"%str(addr))
        
def sendMessage(opcode, message, peernum):
    """Sends a message without blocking. May throw error on timeout"""
    if not hasattr(sendMessage, 'peers'):
        sendMessage.peers = {}
        sendMessage.threads = []
        with open('peers.txt') as f:
            for i, ip in enumerate(filter(lambda x: x != '', f.read().split('\n'))):
                sendMessage.peers[i+1] = (ip.strip(), getport(i+1))
    
    message['senderid'] = 0
    message['opcode'] = opcode

    dprint("Sending message: %s to %s"% (str(message), str(peernum)))
    t = threading.Thread(target=sendMsgA, args=(sendMessage.peers[peernum], json.dumps(message), peernum))
    t.start()
    sendMessage.threads.append(t)
        
destPeerID = int(sys.argv[1])

while True:
    s = raw_input("%02d> "%destPeerID)
    if not s:
        break
    parts = s.split(' ')
    if len(parts) < 2:
        print("Invalid command")
    command = {}
    command['opcode'] = parts[0]
    command['filename'] = parts[1]
    command['id'] = time.time()
    if parts[0] == 'append':
        command['line'] = ' '.join(parts[2:])
        
    sendMessage('EVENT', {'event': json.dumps(command)}, destPeerID)
