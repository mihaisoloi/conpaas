'''
# -- ConPaaS fault tolerance --
#
# UDP server for pinging used by the active failure
# detection.
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

import socket
import struct
import sys
from threading import Thread
from concurrent.futures import Future, ThreadPoolExecutor

from conpaas.core.ft.comm import ping
from conpaas.core.ft.comm.ping import PingProtocol
from conpaas.core.ft.comm.udp.client import PingClient
from conpaas.core.ft.detection.active import ActiveDetection

BUFSIZ = 1024
TIMEOUT = 60

class PingServer():
    def __init__(self, ip, port, activeDetection=None):
        self.ip = ip
        self.port = port
        self.activeDetection = activeDetection
        self.executor = ThreadPoolExecutor(max_workers=100)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, self.port))

    def serve(self):
        while True:
            # receive message
            raw_bytes = bytearray(BUFSIZ)
            raw_bytes, addr = self.sock.recvfrom(BUFSIZ)
            type, msg = PingProtocol.decode_msg(raw_bytes)

            # call the method to process the given message type
            self.executor.submit(self.__serve_req, type, msg, addr)

    def __serve_req(self, type, msg, addr):
        res = []
        if(type == ping.MSG_PING3):
            t = Thread(target = self.activeDetection.serve_ping3, args = (res, msg))
            t.start()
            t.join(timeout = TIMEOUT)
        elif(type == ping.MSG_PING4):
            t = Thread(target = self.activeDetection.serve_ping4, args = (res, msg))
            t.start()
            t.join(timeout = TIMEOUT)
        elif(type == ping.MSG_PING_REQ):
            t = Thread(target = self.activeDetection.serve_ping_req, args=(res, eval(msg)))
            t.start()
            t.join(timeout = TIMEOUT)

        # send the response back
        print(str(res))
        sys.stdout.flush()
        try:
            if(len(res) > 0):
                PingClient.send_n(ping.MSG_RESPONSE, res[0], self.sock, addr[0], addr[1])
        except Exception as e:
            print("Exception in ping server "+str(e))
            sys.stdout.flush()
