#!/usr/bin/python

import socket
import json
import sys

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
        s.close()
        sendMessage.peerStatus[peerID] = True
    except:
        sendMessage.peerStatus[peerID] = False
        dprint("could not send message to %s"%str(addr))
        
def sendMessage(opcode, message, peernum):
    """Sends a message without blocking. May throw error on timeout"""
    if not hasattr(sendMessage, 'peers'):
        sendMessage.peers = {}
        sendMessage.threads = []
        sendMessage.peerStatus = {}
        with open('peers.txt') as f:
            for i, ip in enumerate(filter(lambda x: x != '', f.read().split('\n'))):
                sendMessage.peerStatus[i] = True
                sendMessage.peers[i+1] = (ip.strip(), getport(i+1))
    
    message['senderid'] = 0
    message['opcode'] = opcode

    dprint("Sending message: %s to %s"% (str(message), str(peernum)))
    t = threading.Thread(target=sendMsgA, args=(sendMessage.peers[peernum], json.dumps(message), peernum))
    t.start()
    sendMessage.threads.append(t)

destPeerID = sys.argv[1]

while True:
    s = raw_input("> ")
    if not s:
        break
    sendMessage('EVENT', {'event': s}, destPeerID)
