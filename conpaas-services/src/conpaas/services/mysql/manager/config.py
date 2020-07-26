# -*- coding: utf-8 -*-

"""
    :copyright: (C) 2010-2013 by Contrail Consortium.
"""

from conpaas.core.log import create_logger
from conpaas.core.node import ServiceNode

E_ARGS_UNEXPECTED = 0
E_CONFIG_READ_FAILED = 1
E_CONFIG_NOT_EXIST=2
E_UNKNOWN=3
E_ARGS_MISSING = 4
E_ARGS_INVALID=5
E_STATE_ERROR=6

E_STRINGS = [
  'Unexpected arguments %s',
  'Unable to open configuration file: %s',
  'Configuration file does not exist: %s',
  'Unknown error.',
  'Missing argument: %s',
  'Invalid argument: %s',
  'Service in wrong state'
]

'''
Holds information on service nodes.
'''
class SQLServiceNode(ServiceNode):
    ''' Initializes service node.

    :param vm: Service node id
    :type vm: array
    :param runMySQL: Indicator if service node is running MySQL
    :type runMySQL: boolean

    '''

    def __init__(self, node, isMain=False, isSubordinate=False):
        ServiceNode.__init__(self, node.id,
                             node.ip, node.private_ip,
                             node.cloud_name)
        #self.name = vm['name']
        #self.state = vm['state']
        self.isMain = isMain
        self.isSubordinate = isSubordinate
        self.port = 5555

    '''String representation of the ServiceNode.
    @return: returns service nodes information. Id ip and if mysql is running on this service node.'''

    def __repr__(self):
        return 'ServiceNode(id=%s, ip=%s, main=%s)' % (str(self.id), self.ip, str(self.isMain))
  
class Configuration(object):

    MYSQL_PORT = 3306

    # The port on which the agent listents
    AGENT_PORT = 5555

    '''Representation of the deployment configuration'''
    def __init__(self, configuration):
        self.logger = create_logger(__name__)
        self.mysql_count = 0
        self.serviceNodes = {}        
      
    '''Returns the list of service nodes which are registered in the configuration.'''
    def getMySQLServiceNodes(self):
        return [ serviceNode for serviceNode in self.serviceNodes.values() ]        
        
    '''Returns the list of service nodes as tuples <IP, PORT>.'''
    def getMySQLTuples(self):
        return [ [serviceNode.ip, self.MYSQL_PORT] for serviceNode in self.serviceNodes.values() ]
      
    ''' Returns the list of IPs of MySQL instances'''
    def getMySQLIPs(self):
        return [ serviceNode.ip for serviceNode in self.serviceNodes.values() ]

    ''' Returns the list of MySQL mains'''
    def getMySQLmains(self):
        return [ serviceNode for serviceNode in self.serviceNodes.values() if serviceNode.isMain ]    

    ''' Returns the list of MySQL subordinates'''
    def getMySQLsubordinates(self):
        return [ serviceNode for serviceNode in self.serviceNodes.values() if serviceNode.isSubordinate ] 

    def getMySQLNode(self, id):
        return self.serviceNodes[id]

    '''
      Add new Service Node to the server (configuration).
      @param accesspoint: new VM
    '''
    def addMySQLServiceNodes(self, nodes, isMain=False, isSubordinate=False):
        self.logger.debug('Entering addMySQLServiceNode')
        for node in nodes:
            self.serviceNodes[node.id] = SQLServiceNode(node, isMain, isSubordinate)
        self.logger.debug('Exiting addMySQLServiceNode')

    '''
      Remove Service Node to the server (configuration).
    '''
    def removeMySQLServiceNode(self, id):
        del self.serviceNodes[id]

    def remove_nodes(self, nodes):
        for node in nodes:
            del self.serviceNodes[node.id]
