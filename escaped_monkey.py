#!/usr/bin/python2

import socket
import sys
import select
import threading
import json
import time
import os

import persist

# Constants
DEFAULTPORT = 60000
if os.path.isfile("port"):
    with open("port", 'r') as f:
        DEFAULTPORT = int(f.read().strip())

TIMEOUT_BULLY_OK = 2.0
TIMEOUT_BULLY_COORDINATOR = 2.0
TIMEOUT_DISCOVERY_NEWEPOCH = 3.0
TIMEOUT_DISCOVERY_FOLLOWERINFO = 2.0
TIMEOUT_DISCOVERY_ACKEPOCH = 2.0
TIMEOUT_SYNCHRO_ACKNEWLEADER = 2.0
TIMEOUT_SYNCHRO_NEWLEADER = 5.0
TIMEOUT_SYNCHRO_COMMIT = 4.0

TIMEOUT_HEARTBEAT_LEADER = 0.25
TIMEOUT_HEARTBEAT_FOLLOWER = 0.25

# Utility functions
def dprint(s):
    if hasattr(dprint, 'number'):
        sys.stderr.write("%02d: %s"%(dprint.number, s) + '\n')
    else:
        sys.stderr.write(str(s)+'\n')

#core of the sendMessage function, does the network work
def sendMsgA(addr, msg, peerID):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(addr)
        s.send(msg)
        s.close()
        sendMessage.peerStatus[peerID] = True
    except:
        sendMessage.peerStatus[peerID] = False
        if json.loads(msg)['opcode'] != 'HEARTBEAT':
            dprint("could not send message to %s"%str(addr))

#logical layer of the sendMessage function, coordinates all the message sending
def sendMessage(s, opcode, message, peernum):
    """Sends a message without blocking. May throw error on timeout"""
    if not hasattr(sendMessage, 'peers'):
        sendMessage.peers = {}
        sendMessage.threads = []
        sendMessage.peerStatus = {}
        with open('peers.txt') as f:
            for i, ip in enumerate(filter(lambda x: x != '', f.read().split('\n'))):
                sendMessage.peerStatus[i+1] = True
                sendMessage.peers[i+1] = (ip.strip(), getport(i+1))

    message['senderid'] = s.peerID
    message['opcode'] = opcode
    if message['opcode'] != 'HEARTBEAT':
        dprint("Sending message: %s to %s"% (str(message), str(peernum)))
    t = threading.Thread(target=sendMsgA, args=(sendMessage.peers[peernum], json.dumps(message), peernum))
    t.start()
    sendMessage.threads.append(t)

    
#asynchronous timeout loop on message receipt times    
def timeloop(socket, t):
    timeleft = t
    while timeleft > 0.01:
        st = time.time()
        ir, outr, er = select.select([socket], [], [], timeleft)
        timeleft -= time.time() - st
        for sock in ir:
            con, addr = sock.accept()
            msg = json.loads(con.recv(2**16))
            if msg['opcode'] != 'HEARTBEAT':
                dprint("Recieved message: %s"%str(msg))
            msg['con'] = con
            yield msg


# Helper functions
def getport(peerID):
    return DEFAULTPORT + peerID

# Filesystem functions
filesystem = {}
def deliver(s, message):
    return_string = ''
    dprint("DELIVERING: %s"%str(message))
    m = json.loads(message)
    if m['opcode'] == 'create':
        if m['filename'] in filesystem:
            return_string = "File %s already exists"%m['filename']
        else:
            filesystem[m['filename']] = ''
            return_string = "Done"

    elif m['opcode'] == 'delete':
        if not m['filename'] in filesystem:
            return_string = "File %s does not exist"%m['filename']
        else:
            del filesystem[m['filename']]
            return_string = "Done"

    elif m['opcode'] == 'read':
        if not m['filename'] in filesystem:
            return_string = "File %s does not exist"%m['filename']
        else:
            return_string = filesystem[m['filename']]

    elif m['opcode'] == 'append':
        if not m['filename'] in filesystem:
            return_string = "File %s does not exist"%m['filename']
        else:
            filesystem[m['filename']] += m['line']
            filesystem[m['filename']] += '\n'
            return_string = "Done."

    else:
        return_string = "Command not recognized"

    if m['id'] in s.clients:
        try:
            s.clients[m['id']].send(return_string)
            s.clients[m['id']].close()
        except:
            dprint("Could not reply to client. Is socket dead?")
        del s.clients[m['id']]


# FSM States
def leaderElection(s):
    s.leader = None

    for peerid in s.peers:
        if peerid > s.peerID:
            sendMessage(s, 'ELECTION', {}, peerid)

    peersleft = len(filter(lambda x: x > s.peerID, s.peers))
    #if peersleft != 0:
    for msg in timeloop(s.sock, TIMEOUT_BULLY_OK):
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

    for msg in timeloop(s.sock, TIMEOUT_BULLY_COORDINATOR):
        if msg['opcode'] == 'COORDINATOR':
            s.leader = msg['senderid']
            return "discovery"
        elif msg['opcode'] == 'ELECTION':
            if msg['senderid'] < s.peerID:
                sendMessage(s, 'OK', {}, msg['senderid'])
        else:
            dprint("extranious message")

    return "leader_election"

#discovery fucntion that followers use
def discovery_follower(s):
    sendMessage(s, 'FOLLOWERINFO', {'acceptedEpoch': s.acceptedEpoch}, s.leader)
    for msg in timeloop(s.sock, TIMEOUT_DISCOVERY_NEWEPOCH):
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

#discovery function that leaders use
def discovery_leader(s):
    peersleft = len(s.peers) - 1
    epochnumbers = [s.currentEpoch]
    s.quorum = {}
    contacts = 1
    for msg in timeloop(s.sock, TIMEOUT_DISCOVERY_FOLLOWERINFO):
        if msg['opcode'] == 'FOLLOWERINFO':
            epochnumbers.append(msg['acceptedEpoch'])
            s.quorum[msg['senderid']] = {}
            peersleft -= 1
            contacts += 1
            if peersleft == 0:
                break

        elif msg['opcode'] == 'ELECTION':
            sendMessage(s, 'OK', {}, msg['senderid'])
            return "leader_election"
        elif msg['opcode'] == 'COORDINATOR':
            s.leader = msg['senderid']
            return 'discovery'

    connectedPeers = filter(lambda x: sendMessage.peerStatus[x], sendMessage.peerStatus)
    if contacts <= len(connectedPeers)/2.0:
        dprint("Failed to achive quorum. Contacts: %d, ConnectedPeers: %s"%(contacts, connectedPeers))
        return 'leader_election'

    s.eprime = max(epochnumbers) + 1
    for i in s.quorum:
        sendMessage(s, 'NEWEPOCH', {'eprime': s.eprime}, i)
        s.acceptedEpoch = s.eprime

    peersleft = len(s.quorum)
    for msg in timeloop(s.sock, TIMEOUT_DISCOVERY_ACKEPOCH):
        if msg['opcode'] == 'ACKEPOCH':
            if msg['senderid'] in s.quorum:
                s.quorum[msg['senderid']]['currentEpoch'] = msg['currentEpoch']
                s.quorum[msg['senderid']]['lastZxid'] = msg['lastZxid']
                s.quorum[msg['senderid']]['history'] = msg['history']
                peersleft -= 1
                if peersleft == 0:
                    break

        elif msg['opcode'] == 'ELECTION':
            sendMessage(s, 'OK', {}, msg['senderid'])
            return "leader_election"
        elif msg['opcode'] == 'COORDINATOR':
            s.leader = msg['senderid']
            return 'discovery'

    if peersleft != 0:
        return 'leader_election'

    q = s.quorum.copy()
    q[s.peerID] = {'currentEpoch': s.currentEpoch,
                   'lastZxid': s.lastZxid,
                   'history': s.history[:]}
    highestEpoch = max([q[i]['currentEpoch'] for i in q])
    hEg = filter(lambda x: q[x]['currentEpoch'] == highestEpoch, q)
    highestZxid = max([q[i]['lastZxid'] for i in hEg])
    hZg = filter(lambda x: q[x]['lastZxid'] == highestZxid, hEg)
    f = hZg[0]
    dprint("f has been chosen as %d"%f)
    s.history.purge()
    for e in q[f]['history']:
        if e == '':
            continue
        s.history.append(json.dumps((s.eprime, json.loads(e)[1])))

    return 'synchronization'

#overall discovery function, delegate to correct function based on follower/leader
def discovery(s):
    global filesystem
    filesystem = {}
    if s.peerID == s.leader:
        return discovery_leader(s)
    else:
        return discovery_follower(s)

#synchronization function that leaders use
def synchronization_leader(s):
    for i in s.quorum:
        sendMessage(s, 'NEWLEADER', {'eprime': s.eprime,
                                     'history': s.history}, i)
        s.currentEpoch = s.eprime

    peersleft = len(s.quorum)
    for msg in timeloop(s.sock, TIMEOUT_SYNCHRO_ACKNEWLEADER):
        if msg['opcode'] == 'ACKNEWLEADER':
            peersleft -= 1
            if peersleft == 0:
                break

    if len(s.quorum)-peersleft+1 <= len(s.peers)/2.0:
        dprint("Synchronization leader failed to achive a quorum, got %d / %d"% (len(s.quorum)-peersleft, len(s.peers)))
        return 'leader_election'

    for i in s.peers:
        sendMessage(s, 'COMMIT', {}, i)

    #TODO: WRONG EPOCH ON LEADER HISTORY
    for item in s.history:
        if item == '':
            continue
        deliver(s, json.loads(item)[1][0])


    return 'broadcast'

#synchronization function that followers use
def synchronization_follower(s):
    noncommited_txns = {}
    to_commit_txns = {}
    recvdLeader = False
    for msg in timeloop(s.sock, TIMEOUT_SYNCHRO_NEWLEADER):
        if msg['opcode'] == 'NEWLEADER':
            if msg['senderid'] == s.leader:
                if msg['eprime'] == s.acceptedEpoch:
                    recvdLeader = True
                    s.currentEpoch = msg['eprime']
                    # purge history
                    s.history.purge()
                    for proposal in msg['history']:
                        if proposal == '':
                            continue
                        dprint("Proposal: %s"% str(proposal))
                        #TODO: sort before adding to history
                        s.history.append(json.dumps((s.currentEpoch, json.loads(proposal)[1])))
                        #TODO: make dict?
                        noncommited_txns[tuple(json.loads(proposal)[1][1])] = json.loads(proposal)[1][0]

                    sendMessage(s, 'ACKNEWLEADER', {'eprime': msg['eprime'],
                                                    'history': msg['history']}, s.leader)
                    break
                else:
                    return 'leader_election'

        else:
            dprint("Extranious message during follower synchronization")

    if not recvdLeader:
        return 'leader_election'

    for msg in timeloop(s.sock, TIMEOUT_SYNCHRO_COMMIT):
        if msg['opcode'] == 'COMMIT':
            if msg['senderid'] == s.leader:
                #TODO: sort noncommited_txns
                for tx in sorted(noncommited_txns.items()):
                    deliver(s, tx[1])
                return 'broadcast'

        else:
            dprint("Extranious message during follower synchronazation commit wait")

    return 'leader_election'

#overall synchronization function, delegate to correct function based on follower/leader
def synchronization(s):
    if s.peerID == s.leader:
        return synchronization_leader(s)
    else:
        return synchronization_follower(s)

#broadcast function that leaders use
def broadcast_leader(s):
    counter = 0
    ackcounts = {}
    while True:
        for i in s.peers:
            sendMessage(s, 'HEARTBEAT', {}, i)

        for msg in timeloop(s.sock, TIMEOUT_HEARTBEAT_LEADER):

            if msg['opcode'] == 'EVENT':
                if msg['senderid'] == 0:
                    s.clients[json.loads(msg['event'])['id']] = msg['con']
                counter += 1
                event = json.dumps((s.eprime, (msg['event'],  (s.eprime, counter))))
                for i in s.quorum:
                    sendMessage(s, 'PROPOSE', {'event': event}, i)
                ackcounts[event] = 1
                s.history.append(event)

            elif msg['opcode'] == 'ACKEVENT':
                if not msg['event'] in ackcounts:
                    continue

                ackcounts[msg['event']] += 1

                if ackcounts[msg['event']] > len(s.peers) / 2.0:
                    for i in s.peers:
                        if i == s.peerID:
                            continue
                        sendMessage(s, 'COMMITTX', {'event': msg['event']}, i)
                        deliver(s, json.loads(msg['event'])[1][0])
                    del ackcounts[msg['event']]

            elif msg['opcode'] == 'FOLLOWERINFO':
                sendMessage(s, 'NEWEPOCH', {'eprime': s.eprime}, msg['senderid'])
                sendMessage(s, 'NEWLEADER', {'eprime': s.eprime, 'history': s.history}, msg['senderid'])

            elif msg['opcode'] == 'ACKNEWLEADER':
                sendMessage(s, 'COMMIT', {}, msg['senderid'])
                if not msg['senderid'] in s.quorum:
                    s.quorum.append(msg['senderid'])

            elif msg['opcode'] == 'ELECTION':
                sendMessage(s, 'OK', {}, msg['senderid'])
                return "leader_election"

            elif msg['opcode'] == 'COORDINATOR':
                s.leader = msg['senderid']
                return 'discovery'

        connectedPeers = filter(lambda x: sendMessage.peerStatus[x], sendMessage.peerStatus)
        if len(connectedPeers) <= len(s.peers)/2.0:
            return "leader_election"

#broadcast function that followers use
def broadcast_follower(s):
    noncommited_txns = {}
    to_commit_txns = {}
    while True:
        sendMessage(s, 'HEARTBEAT', {}, s.leader)
        for msg in timeloop(s.sock, TIMEOUT_HEARTBEAT_FOLLOWER):
            if msg['opcode'] == 'PROPOSE':
                s.history.append(msg['event'])
                noncommited_txns[tuple(json.loads(msg['event'])[1][1])] = json.loads(msg['event'])[1][0]
                sendMessage(s, 'ACKEVENT', {'event': msg['event']}, s.leader)

            elif msg['opcode'] == 'COMMITTX':
                to_commit_txns[tuple(json.loads(msg['event'])[1][1])]=json.loads(msg['event'])[1][0]
                if len(noncommited_txns) == 0:
                    continue
                while min(noncommited_txns) == min(to_commit_txns):
                    deliver(s, to_commit_txns[min(to_commit_txns)])
                    del noncommited_txns[min(noncommited_txns)]
                    del to_commit_txns[min(to_commit_txns)]
                    if len(noncommited_txns) == 0 or len(to_commit_txns) == 0:
                        break


            elif msg['opcode'] == 'ELECTION':
                sendMessage(s, 'OK', {}, msg['senderid'])
                return "leader_election"
            elif msg['opcode'] == 'COORDINATOR':
                s.leader = msg['senderid']
                return 'discovery'

            elif msg['opcode'] == 'EVENT':
                if msg['senderid'] == 0:
                    s.clients[json.loads(msg['event'])['id']] = msg['con']
                sendMessage(s, 'EVENT', {'event': msg['event']}, s.leader)

        if not sendMessage.peerStatus[s.leader]:
            return 'leader_election'

#overall broadcast function, delegate to correct function based on follower/leader
def broadcast(s):
    if s.leader == s.peerID:
        return broadcast_leader(s)
    else:
        return broadcast_follower(s)

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
    s.clients = {}
    with open('peers.txt') as f:
        for i, ip in enumerate(filter(lambda x: x != '', f.read().split('\n'))):
            s.peers[i+1] = (ip.strip(), getport(i+1))

    s.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.sock.bind(('0.0.0.0', getport(peerID)))
    s.sock.listen(20)

    state = "leader_election"
    while True:
        dprint("Going to state: %s"%state)
        state = states[state](s)

if __name__ == "__main__":
    main()
