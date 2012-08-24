'''
# -- ConPaaS fault tolerance --
#
# The EntryPoint class initializes the fault tolerance process
# on the VM it is called for
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

from conpaas.core.ft.membership.GossipPeerSampling import GossipPeerSampling, NodeDescriptor
from conpaas.core.ft.detection.active.ActiveDetection import ActiveDetection
from conpaas.core.ft.server import FTServer
from conpaas.core.ft.comm.udp.server import PingServer
from conpaas.core.ft.ReplicateState import ReplicateState
from conpaas.core.ft.recovery.Recovery import Recovery

from threading import Thread

from conpaas.core.abstract import client3

from conpaas.core.ft.dht import client3 as dht_client3

import sys, binascii

import os.path, subprocess

class EntryPoint(object):
    def __init__(self, server_address, **kwargs):
        # initialize the memebership component
        print(kwargs['peers'])
        membership = self.start_membership(kwargs)
        #initialize the failure detection
        activeDetection = self.start_adetection(membership, kwargs)
        #initialize the failure recovery
        recovery = self.get_recovery(kwargs)
        # initialize the fault tolerance server
        ftserver = FTServer(server_address, membership, recovery)
        # initialize the ping server used by the membership component
        pingserver = PingServer("0.0.0.0", 5002, activeDetection)

        # start DHT as a new process
        self.start_dht(kwargs)

        # start state replication thread
        stateRep = self.state_rep(kwargs)

        try:
            t1 = Thread(target=ftserver.serve_forever, args=())
            t1.start()
            t4 = Thread(target=pingserver.serve, args=())
            t4.start()
            t2 = Thread(target = membership.client, args=())
            t2.start()
            t3 = Thread(target = activeDetection.client, args=[])
            t3.start()
            t5 = Thread(target = stateRep.work, args=())
            t5.start()
        except:
            print("Error: unable to start thread")

        # wait for all the threads
        t1.join()
        t4.join()
        t2.join()
        t3.join()
        t5.join()

    def start_membership(self, kwargs):
        peerlist = []
        myip = kwargs['myip']
        myport = kwargs['myport']
        peers = kwargs['peers']
        # for each peer
        for i in range (0, len(peers)):
            IP, port = peers[i].split(':')
            # get roles running on it
            roles = client3.get_roles(IP, int(port))
            for j in range (0, len(roles)):
                ID, name, role_port = roles[j].split(':')
                descr = NodeDescriptor.build_descriptor(IP, role_port, ID, name)
                # insert role descriptor into peerlist
                peerlist.append(NodeDescriptor(descr))

        membership = GossipPeerSampling(myip, myport, peerlist, 1, 6, 2, 4)
        return membership

    def start_adetection(self, membership, kwargs):
        config_parser = kwargs['config_parser']
        myip = kwargs['myip']
        myport = kwargs['myport']
        peers = kwargs['peers']
        mydescr = NodeDescriptor.build_descriptor(myip, myport, '', '')
        manager_ip = ''
        if(len(peers) == 0):
            manager_ip = myip
        else:
            manager_ip = peers[len(peers)-1].split(':')[0]
        activeDetection = ActiveDetection(mydescr, manager_ip, 80, membership, 11, 5, 10, 2, config_parser)
        return activeDetection

    def start_dht(self, kwargs):
        myip = kwargs['myip']
        mykey = dht_client3.get_key(myip)
        bootstrap_ip = kwargs['bootstrap_ip']
        bootstrap_port = kwargs['bootstrap_port']

        # path to the script starting the DHT as a separate process
        path = os.path.join("/root/ConPaaS/src/conpaas/core/ft/dht", "start_dht.py")
        if(bootstrap_ip != None):
            print("bootstrp "+bootstrap_ip+":"+str(bootstrap_port))
            subprocess.Popen([path, '-a', myip, '-p', '9990', '-k', str(mykey), '-A', bootstrap_ip, '-P', str(bootstrap_port)], close_fds=True)
        else:
            subprocess.Popen([path, '-a', myip, '-p', '9990', '-k', str(mykey)], close_fds=True)

    def state_rep(self, kwargs):
        myip = kwargs['myip']
        myport = kwargs['myport']
        replication = ReplicateState(myip, myport, 10)
        return replication

    def get_recovery(self, kwargs):
        config_parser = kwargs['config_parser']
        myip = kwargs['myip']
        myport = kwargs['myport']
        peers = kwargs['peers']
        manager_ip = ''
        if(len(peers) == 0):
            manager_ip = myip
        else:
            manager_ip = peers[len(peers)-1].split(':')[0]
        recovery = Recovery(myip, myport, manager_ip, 80, config_parser)
        return recovery
