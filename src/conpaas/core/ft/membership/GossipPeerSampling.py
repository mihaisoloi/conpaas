'''
# -- ConPaaS fault tolerance --
#
# Builds random partial views at each node using
# the gossip peer sampling algorithm.
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

# Python 2.5 and later only

from threading import Lock
import time
from random import shuffle
from operator import itemgetter, attrgetter
from collections import deque

from conpaas.core.ft import client
from conpaas.core.http3 import HttpJsonResponse, HttpError

from conpaas.core.abstract import client3

import sys
import json
import random

from conpaas.core.expose import expose


class GossipPeerSampling(object):
    # T is the period time
    # c is the size of the view
    # H, S are algorithm parameters, according to the thesis
    def __init__(self, myip, myport, initial_view, T, c, H, S):
        self.myip = myip
        self.myport = myport
        self.myroles_descr = []
        # ensure initial_view has size at most c
        if len(initial_view) > c :
            initial_view = initial_view[0:c]
        self.partialView =  View(initial_view)
        self.push = True
        self.pull = True
        self.T = T
        self.c = c
        self.H = H
        self.S = S
        self.lock = Lock()
        self.failedRoles = deque([], 10)
        self.failedRolesLock = Lock()

    def client(self):

        while 1:
            try:

                buf = SerBuffer([])

                time.sleep(self.T)

                self.lock.acquire()

                peer = self.partialView.select_peer()
                if(peer == None):
                    self.lock.release()
                    continue

                if(self.push):
                    # refresh list of roles running locally
                    while True:
                        self._refresh_roles()
                        if len(self.myroles_descr) > 0:
                            break

                    # select a random one
                    self.role_descr = self.get_rand_role()
                    buf = SerBuffer([self.role_descr])

                    self.partialView.permute()

                    # move oldest H items to the end of the view
                    self.partialView.move_oldest_n(self.H)

                    # append to buffer the first c/2-1 elements in the view
                    for descr in self.partialView.view[0:int(self.c/2-1)]:
                        buf.buffer.append(descr)

                    self.lock.release()

                    # send buffer to peer
                    peerip = peer.id.split(":")[0]
                    resp = client.exchange_views(peerip, '15000', buf.encode())
                else:
                    # send null to peer
                    emptyBuf = SerBuffer([])
                    resp = client.exchange_views(peerip, '15000', emptyBuf.encode())

                if(self.pull):
                    # response from peer is in resp
                    code, body = resp
                    data = json.loads(body)
                    respBuffer = SerBuffer.decode(data['result'])

                    # remove my IP from received buffer
                    respBuffer.remove_IP(self.role_descr)

                    # remove failed peers from the received buffer
                    try:
                        self.remove_failed_peers(respBuffer)
                    except Exception as e:
                        print(str(e))
                        sys.stdout.flush()

                    self.lock.acquire()

                    self.partialView.select(self.c, self.H, self.S, respBuffer.buffer)

                self.partialView.increase_age()
                print('Partial view: '+str(self.partialView))
                sys.stdout.flush()

                self.lock.release()

            except HttpError as e:
                print("*********** httperr in client "+str(e))
                sys.stdout.flush()
                pass

            except Exception as e:
                print("*** Exception in client "+str(e))
                sys.stdout.flush()

    @expose('POST')
    def server(self, kwargs):
        try:
            # receive buffer from peer
            recvBuffer = SerBuffer.decode(kwargs)

            if(self.pull):

                self.lock.acquire()

                # refresh list of roles running locally
                while True:
                    self._refresh_roles()
                    if len(self.myroles_descr) > 0:
                        break
                # get a random role
                self.role_descr = self.get_rand_role()

                buf = SerBuffer([self.role_descr])
                self.partialView.permute()

                # move oldest H items to the end of the view
                self.partialView.move_oldest_n(self.H)

                for descr in self.partialView.view[0:int(self.c/2-1)]:
                    buf.buffer.append(descr)

                self.lock.release()

                # send to peer
                tosend = buf.encode()

            # remove my IP from received buffer
            recvBuffer.remove_IP(self.role_descr)

            # remove failed peers from the received buffer
            try:
                self.remove_failed_peers(recvBuffer)
            except Exception as e:
                print(str(e))
                sys.stdout.flush()

            self.lock.acquire()

            self.partialView.select(self.c, self.H, self.S, recvBuffer.buffer)

            self.partialView.increase_age()
            print('Partial view: '+str(self.partialView))
            sys.stdout.flush()

            self.lock.release()

            if(self.pull):
                return HttpJsonResponse(tosend)
        except HttpError as e:
            print("%%%%%%%% http err in server "+str(e))
            sys.stdout.flush()
            pass

        except Exception as e:
            print("%%%%%%%%%%%%%%%%%%%%%%% in server exc "+str(e))
            sys.stdout.flush()

    def get_random_peer(self):
        self.lock.acquire()
        if(len(self.partialView.view) == 0):
            self.lock.release()
            return None
        randPos = random.randint(0,len(self.partialView.view)-1)
        res = self.partialView.view[randPos]
        self.lock.release()
        return res

    def get_oldest_peer(self):
        self.lock.acquire()
        if(len(self.partialView.view) == 0):
            self.lock.release()
            return None
        res = self.partialView.select_peer()
        self.lock.release()
        return res

    def reset_age(self, descriptor):
        self.lock.acquire()
        for descr in self.partialView.view:
            if(descr.id == descriptor.id):
                descr.age = 0
                break
        self.lock.release()


    def mark_failed_peer(self, peerDescriptor):
        try:
            # add the peer descriptor to the failed roles
            self.failedRolesLock.acquire()
            self.failedRoles.append(peerDescriptor)
            for descr in self.failedRoles:
                pass
            self.failedRolesLock.release()

            # remove the peer descriptor from the partial view
            self.lock.acquire()
            self.partialView.remove(peerDescriptor)
            for descr in self.partialView.view:
                pass
            self.lock.release()
        except Exception as e:
            print("^^^^^^^^^^^^^^^^^^^ exc "+str(e))
            sys.stdout.flush()

    def remove_failed_peers(self, buffer):
        self.failedRolesLock.acquire()
        for i in range(0, len(self.failedRoles)):
            buffer.remove(self.failedRoles[i])
        self.failedRolesLock.release()

    def _refresh_roles(self):
        self.myroles_descr = []
        myroles = client3.get_roles(self.myip, int(self.myport))
        for i in range (0, len(myroles)):
            ID, name, role_port = myroles[i].split(':')
            descr = NodeDescriptor.build_descriptor(self.myip, role_port, ID, name)
            self.myroles_descr.append(NodeDescriptor(descr))

    def get_rand_role(self):
        if(len(self.myroles_descr) == 0):
            raise Exception("Empty roles list")

        randPos = random.randint(0,len(self.myroles_descr)-1)
        res = self.myroles_descr[randPos]
        return res

# used to store a buffer that can be serialized for partial view transfers
class SerBuffer(object):
    def __init__(self, l):
        self.buffer = l

    def encode(self):
        encoding = {}
        nodes = []

        for descr in self.buffer:
            nodes.append(descr.encode())

        encoding['view'] = nodes

        return encoding

    @staticmethod
    def decode(jsonParam):
        content = jsonParam['view']
        l = []
        for jsonDescr in content:
            descr = NodeDescriptor.decode(jsonDescr)
            l.append(descr)
        return SerBuffer(l)

    def remove(self, descriptor):
        index = 0
        found = False
        for descr in self.buffer:
            if(descr.id == descriptor.id):
                found = True
                break
            index += 1

        if(found):
            del self.buffer[index]

    def remove_IP(self, descriptor):
        index = 0
        while True:
            if index >= len(self.buffer):
                break
            sys.stdout.flush()
            if(self.buffer[index].id.split(':')[0] == descriptor.id.split(':')[0]):
                sys.stdout.flush()
                self.buffer.pop(index)
            else:
                index +=1

class NodeDescriptor(object):
    def __init__(self, id, age=0):
        self.id = id
        self.age = age

    def  __str__(self):
        return '<id='+str(self.id)+',age='+str(self.age)+'>'

    def encode(self):
        encoding = {'node' : str(self.id)}
        return encoding

    @staticmethod
    def build_descriptor(ip, port, role_ID, role_name):
        return ip+":"+port+":"+role_ID+":"+role_name

    def split_descriptor(self):
        components = self.id.split(':')
        return components[0], components[1], components[2], components[3]

    @staticmethod
    def decode(jsonDescr):
        id = str(jsonDescr['node'])     # strip unicode tags
        return NodeDescriptor(id)

class addIndex(object):
    def __init__(self, index=-1):
        self.index = index

class View(object):
    def __init__(self, initial_view):
        self.view = initial_view

    # Randomly reorder the view elements
    def permute(self):
        shuffle(self.view)

    # Select the node with the highest age
    def select_peer(self):
        if(len(self.view) == 0):
            return None
        return sorted(self.view, key=attrgetter('age'), reverse=True)[0]

    def select(self, c, H, S, buffer):
        for descr in buffer:
            self.view.append(descr)

        # remove duplicates from the view, keep only the newest ones
        self.removeDuplicates()

        # remove old items min(H, view.size-c)
        nrItems = min(H, len(self.view) - c)
        if(nrItems < 0):
            nrItems = 0
        itemsLeft = len(self.view) - nrItems
        self.move_oldest_n(nrItems)
        self.view = self.view[0:itemsLeft]

        # remove head min(S, view.size-c)
        nrItems = min(S, len(self.view) - c)
        if(nrItems < 0):
            nrItems = 0
        self.view = self.view[nrItems:]

        # remove at random view.size-c
        nrItems = len(self.view)-c
        if(nrItems < 0):
            nrItems = 0
        while(nrItems > 0):
            randPos = random.randint(0,len(self.view)-1)
            del self.view[randPos]
            nrItems -= 1


    def removeDuplicates(self):
        # add to the NodeDescriptor class a memeber called 'index', used for saving the order of the elements as they are positioned in the patial view
        @addIndex
        class NodeDescriptor:
            pass

        for i in range (0, len(self.view)):
            self.view[i].index = i


        # sort the view and eliminate duplicates
        sorted_view = sorted(self.view, key=attrgetter('id', 'age'))

        overwrite = -1
        for i in range (0, len(sorted_view)-1):
            if sorted_view[i+1].id == sorted_view[i].id:
                if(overwrite == -1):
                    overwrite = i+1
            else:
                if(overwrite != -1):
                    sorted_view[overwrite] = sorted_view[i+1]
                    overwrite += 1

        if(overwrite != -1):
            self.view = sorted_view[0:overwrite]                    # keep only non-duplicates
            self.view = sorted(self.view, key=attrgetter('index'))  # arange duplicates as they were ordered initially in the partial view


    def move_oldest_n(self, n):
        if(n > len(self.view)):
            n = len(self.view)
            #raise RuntimeError('Cannot move more items than the length of the view')
        sorted_view = sorted(self.view, key=attrgetter('age'), reverse=True)[0:n]
        for descr in sorted_view:
            self.view.remove(descr)
            self.view.append(descr)

    def increase_age(self):
        for descr in self.view:
            descr.age += 1

    def __str__(self):
        ret = ''
        for descr in self.view:
            ret += str(descr)+' '
        return ret

    def remove(self, descriptor):
        for i in range (0, len(self.view)):
            if(self.view[i].id == descriptor.id):
                del self.view[i]
                break
