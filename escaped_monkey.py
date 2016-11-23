#!/usr/bin/python2

def leaderElection():
    return "Discovery"

def discovery():
    return "Synchronization"

def synchronization():
    return "broadcast"

def broadcast():
    return "leader_election"

# Janky finite state machine. Could probably just return functions

states = {"leader_election": leaderElection,
          "discovery": discovery,
          "synchronization": Synchronization,
          "broadcast": broadcast}

state = "leader_election"
while True:
    state = states[state]():
