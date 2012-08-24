#!/usr/local/bin/python3.2

# This script starts the fault tolerance process. It is executed by the method start_ft() inside the service's manager.

from conpaas.core.ft.startup import EntryPoint

if __name__ == '__main__':

    from optparse import OptionParser
    from configparser import ConfigParser
    from os.path import exists
    import json
    parser = OptionParser()
    parser.add_option('-c', '--config', type='string', default=None, dest='config')
    options, args = parser.parse_args()
    if not options.config or not exists(options.config):
        print('Failed to find configuration file', file=sys.stderr)
        sys.exit(1)

    config_parser = ConfigParser()
    config_parser.read(options.config)

    peers = eval(config_parser.get('agent', 'PEERS'))
    myip = config_parser.get('agent', 'MY_IP')
    myport = config_parser.get('agent', 'MY_PORT')
    ep = EntryPoint(('0.0.0.0', 15000), config_parser=config_parser, myip=myip, myport=myport, peers=peers, bootstrap_ip=peers[0].split(':')[0], bootstrap_port=9990)
