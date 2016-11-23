#!/usr/bin/python2

import socket
import sys

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

# Persistant state
class WriteList(list):
    """
    List that persists to disk
    Only works on things that can be represented as a string without newlines
    """
    def manual_init(self, peerID):
        self.peerID = peerID
        # TODO: load from disk on init

    def append(self, value):
        with open('history%d'%self.peerID, 'a') as f:
            f.write(value + '\n')
        super(WriteList, self).append(value)

class Peer:
    """
    Just set variables in here naturally. Persisting and loading from
    disk will be handled inside this class.
    """
    def __init__(self, peerID):
        # TODO: load from disk on init
        self.peerID = peerID
        self.history = WriteList()
        self.history.manual_init(peerID)
        self._acceptedEpoch = 0
        self._currentEpoch = 0
        self._lastZxid = 0

    # I know you can do this not 100% sure on the syntax
    @acceptedEpoch.setter
    def acceptedEpoch(self, value):
        with open('acceptedEpoch%d'%self.peerID,'w') as f:
            f.write(str(value))
        self._acceptedEpoch = value
    @acceptedEpoch
    def acceptedEpoch(self):
        return self._acceptedEpoch

    @currentEpoch.setter
    def currentEpoch(self, value):
        with open('currentEpoch%d'%self.peerID,'w') as f:
            f.write(str(value))
        self._currentEpoch = value
    @currentEpoch
    def currentEpoch(self):
        return self._currentEpoch

    @lastZxid.setter
    def lastZxid(self, value):
        with open('lastZxid%d'%self.peerID,'w') as f:
            f.write(str(value))
        self._lastZxid = value
    @lastZxid
    def lastZxid(self):
        return self._lastZxid


# FSM States
def leaderElection(s):
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
    s = Peer()

    state = "leader_election"
    while True:
        state = states[state](s):
