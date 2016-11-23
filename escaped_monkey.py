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
    s = persist.Peer()

    state = "leader_election"
    while True:
        state = states[state](s):
