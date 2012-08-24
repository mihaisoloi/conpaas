'''
# -- ConPaaS fault tolerance --
#
# Implementation of the recovery mechanisms.
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

# Python 2.5 and later only
import time
import sys

from concurrent.futures import thread, process, _base
from threading import Lock

from conpaas.core.ft.detection.active import client
from conpaas.core.expose import expose
from conpaas.core.ft.membership.GossipPeerSampling import NodeDescriptor
from conpaas.core.ft.membership import GossipPeerSampling

from conpaas.core.http3 import HttpJsonResponse, HttpError, HttpErrorResponse

from conpaas.core.abstract import client3

from conpaas.core.ft.dht import client3 as dht_client3

import traceback, json, ast, sys

# constants definition; avoid values that could be valid DHT keys
ROLE_ALREADY_RECOVERED = -1
INCOMPATIBLE_VM = -2

class Recovery(object):
    def __init__(self, myip, myport, manager_ip, manager_port, config_parser):
        self.myip = myip
        self.myport = myport
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.config_parser = config_parser
        self.restart_list = []
        self.lock = Lock()

    def do_pre_recover(self, flag_conflicts, kwargs):
        try:
            descr_to_recover = NodeDescriptor.decode(kwargs['failed_role'])

            failed_ip, failed_port, failed_id, failed_name = descr_to_recover.split_descriptor()
            print("Failed role "+str(descr_to_recover))
            sys.stdout.flush()
            key = dht_client3.get_key(failed_id)

            # notify the manager of the failure
            client3.notify_manager(self.manager_ip, int(self.manager_port), failed_ip, int(failed_port), int(failed_id), failed_name, 'remove')

            if(int(failed_id) in self.restart_list):
                return ROLE_ALREADY_RECOVERED

            if(flag_conflicts == True):
                # get list of incompatibilities of failed role
                incompatible = []
                conflicts = eval(self.config_parser.get('ft_service', 'CONFLICTS'))
                for conflict in conflicts:
                    type1, type2 = conflict.split(':')
                    if(failed_name == type1):
                        incompatible.append(type2)
                    elif(failed_name == type2):
                        incompatible.append(type1)
                # get roles running here and test incompatibilities
                roles = client3.get_roles(self.myip, int(self.myport))
                for j in range (0, len(roles)):
                    ID, name, role_port = roles[j].split(':')
                    if(name in incompatible):
                        return INCOMPATIBLE_VM

            # request state localy
            state = dht_client3.get((self.myip, 9990), key)

            # store state in temp file
            path = '/tmp/'+str(key)
            file = open(path, 'wb')
            file.write(state)
            file.close()
        except Exception as e:
            print('IN PRE RECOVERY'+str(e))

    @expose('POST')
    def recover_on_new_VM(self, kwargs):
        try:
            self.lock.acquire()

            value = self.do_pre_recover(False, kwargs)
            if(value == ROLE_ALREADY_RECOVERED):
                return HttpJsonResponse({'recovered':True})
            elif(value == INCOMPATIBLE_VM):
                return HttpJsonResponse({'recovered':False})

            descr_to_recover = NodeDescriptor.decode(kwargs['failed_role'])

            failed_ip, failed_port, failed_id, failed_name = descr_to_recover.split_descriptor()
            key = dht_client3.get_key(failed_id)

            # start role on manager = itself
            path = '/tmp/'+str(key)
            client3.start_role(self.manager_ip, int(self.manager_port), failed_name, 0, path, new = True)
            self.restart_list.append(int(failed_id))

            self.lock.release()
            return HttpJsonResponse({'recovered':True})
        except Exception as e:
            self.lock.release()
            print('IN RECOVERY'+str(e))
            return HttpErrorResponse('IN RECOVERY'+str(e))

    @expose('POST')
    def recover_mixed(self, kwargs):
        descr_to_recover = NodeDescriptor.decode(kwargs['failed_role'])
        pass

    @expose('POST')
    def recover_on_existent_VM(self, kwargs):
        try:
            self.lock.acquire()

            value = self.do_pre_recover(True, kwargs)
            if(value == ROLE_ALREADY_RECOVERED):
                return HttpJsonResponse({'recovered':True})
            elif(value == INCOMPATIBLE_VM):
                return HttpJsonResponse({'recovered':False})

            descr_to_recover = NodeDescriptor.decode(kwargs['failed_role'])

            failed_ip, failed_port, failed_id, failed_name = descr_to_recover.split_descriptor()
            key = dht_client3.get_key(failed_id)

            # restartrole on current node
            path = '/tmp/'+str(key)
            new_role_id = client3.get_role_id(self.manager_ip, int(self.manager_port))

            path = '/tmp/'+str(key)

            client3.start_role(self.myip, int(self.myport), failed_name, int(new_role_id), path)
            self.restart_list.append(int(failed_id))

            # notify the manager about the addition
            client3.notify_manager(self.manager_ip, int(self.manager_port), self.myip, int(failed_port), int(new_role_id), failed_name, 'add')

            self.lock.release()
            return HttpJsonResponse({'recovered':True})
        except Exception as e:
            self.lock.release()
            print('IN RECOVERY'+str(e))
            return HttpErrorResponse('IN RECOVERY'+str(e))
