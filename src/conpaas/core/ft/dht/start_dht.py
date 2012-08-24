#!/usr/bin/python

from conpaas.core.ft.dht import server
from optparse import OptionParser
from ConfigParser import ConfigParser

import sys

if __name__ == '__main__':
  
    parser = OptionParser()
    parser.add_option('-a', '--ip_address', type='string', default='0.0.0.0', dest='ip_address')
    parser.add_option('-p', '--port', type='int', default=9990, dest='port')
    parser.add_option('-k', '--key', type='int', default=0, dest='key')
    parser.add_option('-A', '--bootstrap_ip', type='string', default=None, dest='bootstrap_ip')
    parser.add_option('-P', '--bootstrap_port', type='int', default=None, dest='bootstrap_port')

    options, args = parser.parse_args()

    address = (options.ip_address, options.port)
    print address
    if options.bootstrap_ip == None:
        peer = server.Server(address, options.key)
    else:
        remote_address = (options.bootstrap_ip, options.bootstrap_port)
        peer = server.Server(address, options.key, remote_address)

    peer.serve_forever()
