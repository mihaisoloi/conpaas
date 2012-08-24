'''
# -- ConPaaS fault tolerance --
#
# Client for the active failure detection
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

from conpaas.core.ft.comm.udp.client import PingClient
from conpaas.core.ft.comm import ping

import traceback

from concurrent.futures._base import TimeoutError

import sys

def _check(response):
    code, body = response
    if code != http.client.OK:
        print("answer code not ok, raising agent exception")
        sys.stdout.flush()
        raise AgentException('Received http response code %d' % (code))

    try:
        data = json.loads(body)
    except Exception as e:
        print("exception when loading json body")
        sys.stdout.flush()
        raise AgentException(*e.args)

    if data['error']:
        print("exception due to data[error]")
        sys.stdout.flush()
        raise AgentException(data['error'])
    else:
        return True


def ping3(host, port, src, dest, epoch):
    pc = PingClient(host, port)
    msg_type = ping.MSG_PING3
    params = {'src' : src, 'dest' : dest, 'epoch' : epoch, }

    resp = pc.send_msg(msg_type, str(params))
    return resp

def ping4(host, port, src, dest, original_src, epoch):
    pc = PingClient(host, port)
    msg_type = ping.MSG_PING4
    params = {'src' : src, 'dest' : dest, 'original_src' : original_src, 'epoch' : epoch, }

    resp = pc.send_msg(msg_type, str(params))
    return resp

def ping_req(host, port, original_src, dest, epoch):
    pc = PingClient(host, port)
    msg_type = ping.MSG_PING_REQ
    params = {'original_src' : original_src, 'dest' : dest, 'epoch' : epoch, }

    resp = pc.send_msg(msg_type, str(params))
    return resp
