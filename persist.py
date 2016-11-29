#!/usr/bin/python

import os

# Persistant state
class WriteList(list):
    """
    List that persists to disk
    Only works on things that can be represented as a string without newlines
    """
    def manual_init(self, peerID):
        self.peerID = peerID
        if os.path.isfile('history%d'%self.peerID):
             with open('history%d'%self.peerID, 'r') as f:
                 for l in f.read().split('\n'):
                     super(WriteList, self).append(l)

    def append(self, value):
        with open('history%d'%self.peerID, 'a') as f:
            f.write(value + '\n')
        super(WriteList, self).append(value)

    def purge(self):
        with open('history%d'%self.peerID, 'w') as f:
            f.write('')
        del self[:]

class Peer(object):
    """
    Just set variables in here naturally. Persisting and loading from
    disk will be handled inside this class.
    """
    def __init__(self, peerID):
        self.peerID = peerID
        self.history = WriteList()
        self.history.manual_init(peerID)

        # TODO: load from disk on init
        if os.path.isfile('acceptedEpoch%d'%self.peerID):
            with open('acceptedEpoch%d'%self.peerID,'r') as f:
                self._acceptedEpoch = int(f.read())
        else:
            self._acceptedEpoch = 0

        if os.path.isfile('currentEpoch%d'%self.peerID):
            with open('currentEpoch%d'%self.peerID,'r') as f:
                self._currentEpoch = int(f.read())
        else:
            self._currentEpoch = 0


        if os.path.isfile('lastZxid%d'%self.peerID):
            with open('lastZxid%d'%self.peerID,'r') as f:
                self._lastZxid = int(f.read())
        else:
            self._lastZxid = 0

    # I know you can do this not 100% sure on the syntax
    @property
    def acceptedEpoch(self):
        return self._acceptedEpoch
    @acceptedEpoch.setter
    def acceptedEpoch(self, value):
        with open('acceptedEpoch%d'%self.peerID,'w') as f:
            f.write(str(value))
        self._acceptedEpoch = value

    @property
    def currentEpoch(self):
        return self._currentEpoch
    @currentEpoch.setter
    def currentEpoch(self, value):
        with open('currentEpoch%d'%self.peerID,'w') as f:
            f.write(str(value))
        self._currentEpoch = value

    @property
    def lastZxid(self):
        return self._lastZxid
    @lastZxid.setter
    def lastZxid(self, value):
        with open('lastZxid%d'%self.peerID,'w') as f:
            f.write(str(value))
        self._lastZxid = value
