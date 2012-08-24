'''
# -- ConPaaS fault tolerance --
#
# UDP client for pinging used by the active failure
# detection.
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

import socket
import sys

from conpaas.core.ft.comm.ping import PingProtocol

BUFSIZ = 1024

class PingClient():
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


    def send_msg(self, msg_type, msg):
        msg_bytes = PingProtocol.encode_msg(msg_type, msg)

        PingClient.__send_n(len(msg_bytes), msg_bytes, self.sock, self.ip, self.port)

        raw_bytes = bytearray(BUFSIZ)
        raw_bytes, addr = self.sock.recvfrom(BUFSIZ)
        type, msg = PingProtocol.decode_msg(raw_bytes)

        return msg

    @staticmethod
    def send_n(msg_type, msg, sock, ip, port):
        try:
            msg_bytes = PingProtocol.encode_msg(msg_type, msg)
        except Exception as e:
            print(str(e))
        sys.stdout.flush()
        PingClient.__send_n(len(msg_bytes), msg_bytes, sock, ip, port)

    @staticmethod
    def __send_n(n, buf_bytes, sock, ip, port):
        sent = 0
        while(n>0):
            s = sock.sendto(buf_bytes[sent:], (ip, port))
            n -= s
            sent += s
