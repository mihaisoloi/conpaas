'''
# -- ConPaaS fault tolerance --
#
# Active failure detection
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

# Python 2.5 and later only
import time
import sys

from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures._base import TimeoutError
from concurrent.futures._base import CancelledError
import concurrent.futures
from concurrent.futures import thread, process, _base


from conpaas.core.ft.detection.active import client
from conpaas.core.ft import client as ft_client
from conpaas.core.expose import expose
from conpaas.core.ft.membership.GossipPeerSampling import NodeDescriptor
from conpaas.core.ft.membership import GossipPeerSampling

from conpaas.core.http3 import HttpJsonResponse, HttpError

from conpaas.core.abstract import client3

from conpaas.core.ft.dht import client3 as dht_client3

import traceback, json, ast, sys

class ActiveDetection(object):
    # detection runs every T seconds
    # time to wait for timeouts in total is periodTime
    # k is the number of intermediate peers to ping
    # all the above parameters are explained in the thesis
    def __init__(self, id, manager_ip, manager_port, peerSamplingService, T, maxRTT, periodTime, k, config_parser):
        self.peerSampling = peerSamplingService
        self.mi = NodeDescriptor(id)
        self.maxRTT = maxRTT
        self.T = T
        self.epoch = 0
        self.periodTime = periodTime
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.k = k
        self.manager_ip = manager_ip
        self.manager_port = manager_port
        self.config_parser = config_parser

    def client(self):
        while 1:

            time.sleep(self.T)

            self.epoch += 1

            # select the OLDEST member from view to test for failure
            mj = self.peerSampling.get_oldest_peer()
            if(mj == None):
                continue
            print ('$$$$$ selected peer '+str(mj))
            sys.stdout.flush()

            # get configuration for role
            name = mj.id.split(':')[3]
            role_section = 'role_'+name
            detection = self.config_parser.get(role_section, 'DETECTION')
            recovery = self.config_parser.get(role_section, 'RECOVERY')
            ## do not perform active detection on role with another type
            ## of configured detection
            if(detection != 'active'):
                continue

            # send ping (mi, mj, epoch) to mj
            peerip = mj.id.split(":")[0]

            try:
                future = self.executor.submit(client.ping3, peerip, 5002, self.mi.encode(), mj.encode(), self.epoch)
            except Exception as e:
                print("exception in activedet "+str(e))
                print(e.__traceback__)
                sys.stdout.flush()
            try :
                print("$$$$$ submitted, waiting for answer "+str(self.maxRTT))
                sys.stdout.flush()
                res = future.result(timeout=self.maxRTT)
                print ('$$$$$ response from peer '+str(res))
                sys.stdout.flush()

                # reset peer age to 0
                self.peerSampling.reset_age(mj)
            except TimeoutError:
                # cancel the future
                print ('$$$$$ timeout ')
                sys.stdout.flush()
                try:
                    res = future.cancel()
                    print("cancelled future "+str(res))
                    sys.stdout.flush()
                except CancelledError:
                    print("cancelled error")
                    sys.stdout.flush()
                    pass

                # select k members randomly from the view
                members = []
                tries = 0
                for x in range(0,self.k):
                    if(tries > 10):
                        break
                    randPeer = self.peerSampling.get_random_peer()
                    if(randPeer.id != mj.id):
                        print("selected via peer "+str(randPeer))
                        sys.stdout.flush()
                        members.append(randPeer)
                    else:
                        tries += 1

                try:
                    alive = False
                    if (len(members) > 0):
                        # send each a ping (mi, mj, epoch)
                        futures = []
                        for x in range(0,len(members)):
                            peerip = members[x].id.split(":")[0]
                            print("$$$$$ pinging "+str(mj.id)+" via "+str(peerip))
                            sys.stdout.flush()
                            f = self.executor.submit(client.ping_req, peerip, 5002, self.mi.encode(), mj.encode(), self.epoch)
                            futures.append(f)
                        try:
                            results = concurrent.futures.wait(futures, timeout=self.periodTime)
                        except Exception as e:
                            print("Exception in intermediate pings "+str(e))
                            sys.stdout.flush()

                        # cancel uncompleted futures
                        for not_completed in results.not_done:
                            not_completed.cancel()

                        if(len(results.done) == 0):
                            print(str(mj) + 'failed')
                            sys.stdout.flush()
                        else:
                            for fdone in results.done:
                                try:
                                    res = fdone.result()
                                    if(res != None):
                                        alive = True
                                        print("it is alive")
                                        sys.stdout.flush()

                                        # reset peer age to 0
                                        self.peerSampling.reset_age(mj)
                                        break
                                except CancelledError as e:
                                    print("cancelled err "+str(e))
                                    sys.stdout.flush()
                                    pass
                                except TimeoutError as e:
                                    print("timeout err "+str(e))
                                    sys.stdout.flush()
                                    pass

                    if(alive == False):
                        # call method from manager
                        print("RECOVERY begins")
                        encoding = {}
                        encoding['failed_role'] = mj.encode()

                        if(recovery == 'on_new_VM'):
                            # call method on manager to create new VM
                            success = self.recover(self.manager_ip, recovery, encoding)
                        else:
                            # try restart on the successors of the role's key
                            key = dht_client3.get_key(mj.id.split(":")[2])
                            id_first_succ, addr_first_succ = dht_client3.find_successor((self.mi.id.split(':')[0], 9990), key)
                            restart_addr = addr_first_succ[0].decode("utf-8")
                            success = self.recover(restart_addr, recovery, encoding)

                            succs = dht_client3.get_successors((restart_addr, int(addr_first_succ[1])))
                            for succ in succs:
                                if(success == True):
                                    break;
                                restart_addr = succ.split(":")[0]
                                success = self.recover(restart_addr, recovery, encoding)
                                sys.stdout.flush()

                        # remove the failed role mj from the view
                        self.peerSampling.mark_failed_peer(mj)

                except Exception as e:
                    print("Exception in detection "+str(e))
                    sys.stdout.flush()
            except Exception as e:
                print("Exception in outer detection")
                print(e)
                sys.stdout.flush()
                traceback.print_exc()
                sys.stderr.flush()

    def recover(self, restart_ip, recovery, encoding):
        try:
            success = True
            if(recovery == 'on_existing_VM'):
                # avoid recovering roles on the manager
                #if(restart_ip == self.manager_ip):
                #    return False
                success = ft_client.recover_role(restart_ip, 15000, 'existent', encoding)
            elif (recovery == 'on_new_VM'):
                success = ft_client.recover_role(restart_ip, 15000, 'new', encoding)
            else:
                success = ft_client.recover_role(restart_ip, 15000, 'mixed', encoding)
            return success
        except Exception as e:
            # catch restart exception and continue
            print("Recovery exception, retrying elsewhere "+str(e))
            return False

    #mm, mi, epoch#
    def serve_ping3(self, res, kwargs):
        try:
            kwargs = ast.literal_eval(kwargs)
        except Exception as e:
            print(e)
            sys.stdout.flush()

        mj = NodeDescriptor.decode(kwargs['dest'])
        ip, port, ID, name =  mj.split_descriptor()

        try:
            role_state = client3.is_alive(ip, int(port), name, int(ID))

            if role_state == False:
                res = []
                return
            else:
                res.append("OK")
        except Exception as e:
            print(e)
            sys.stdout.flush()


    def serve_ping4(self, res, kwargs):
        kwargs = ast.literal_eval(kwargs)

        mj = NodeDescriptor.decode(kwargs['dest'])
        ip, port, ID, name =  mj.split_descriptor()
        role_state = client3.is_alive(ip, int(port), name, int(ID))

        if role_state == False:
            res = []
            return
        else:
            res.append("OK")

    def serve_ping_req(self, res, kwargs):
        sys.stdout.flush()
        original_src = NodeDescriptor.decode(kwargs['original_src'])
        dest = NodeDescriptor.decode(kwargs['dest'])
        epoch = kwargs['epoch']
        if(dest.id != self.mi.id):
            peerip = dest.id.split(":")[0]
            future = self.executor.submit(client.ping4, peerip, 5002, self.mi.encode(), dest.encode(), original_src.encode(), epoch)
            res = future.result()
