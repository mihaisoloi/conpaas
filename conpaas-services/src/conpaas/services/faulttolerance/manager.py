from conpaas.core.expose import expose
from conpaas.core.ganglia import FaultToleranceGanglia, Datasource
from conpaas.core.manager import BaseManager
from conpaas.services.xtreemfs.manager.manager import XtreemFSManager
from conpaas.core.https.server import HttpJsonResponse
from gmon.ganglia import Ganglia
from time import sleep
from threading import Thread


#TODO: if manager fails restart gmond on all nodes
class FaultToleranceManager(XtreemFSManager):

    def __init__(self, config_parser, **kwargs):
        """ Initializes a fault tolerance manager

            @param config_parser: sets up the service

            @param service_cluster: needed for Ganglia
        """

        BaseManager.__init__(self, config_parser, FaultToleranceGanglia)

        self.logger.debug("Entering FaultToleranceManager initialization")
        #we are using the same contextualization as xtreemfs
        self.controller.generate_context('xtreemfs')
        #we need minimum configuration for ft
        self.controller.config_clouds({"mem": "512", "cpu": "1"})
        self._init_values()
        self.state = self.S_INIT
        self.logger.debug("Leaving FaultToleranceManager initialization")
        self.services = []
        # start checking for service updates
        self.check_for_updates()

    @expose('POST')
    def startup(self, kwargs):
        self.logger.info('FaultToleranceManager starting up')
        return super(FaultToleranceManager, self).startup(kwargs)

    @expose('POST')
    def register(self, kwargs):
        '''
            Registering services to the faulttolerance service, used for both
            adding and deleting services, it contains all the services that
            we should monitor

            @param kwargs: json dict containting a list datasources for ganglia
            @type kwargs: D{'datasources': L{conpaas.core.ganglia.Datasource}}
        '''
        self.logger.debug("Registering new datasources")

        datasources = kwargs["datasources"]
        self.update_ganglia(datasources)

        removedServices = self.classify(
            self.datasource_to_service(datasources))[0]

        for service in removedServices:
            service.shutdown()

        return HttpJsonResponse()

    def classify(self, serviceUpdate):
        '''
            Deciding if a service was removed, or added:
            if removed -> interrupt monitoring, and clean it's history
            if added -> watch for master creation

            @return D{0: L[removed], 1: L[added]}
        '''
        names = lambda y: [x.name for x in y]
        func = lambda x, y: [s for s in x if s.name not in names(y)]

        added = func(serviceUpdate, self.services)
        removed = func(self.services, serviceUpdate)

        self.services.extend(added)
        self.services = func(self.services, removed)

        #this form is necessary for python 2.6
        return dict((k, v) for (k, v) in enumerate([removed, added]))

    def datasource_to_service(self, datasources):
        return [Service.from_dict(datasource)
                for datasource in datasources]

    def update_ganglia(self, datasources=None):
        self.logger.debug("Updating ganglia")
        if datasources:
            self.ganglia.add_datasources(datasources)
        else:
            self.ganglia.add_datasources([s.to_dict() for s in self.services])
        self.ganglia.restart()

    def check_for_updates(self):
        '''
            Verifies if services need to be updated.

            New master added, or failed service node, manager unreachable'
        '''
        def check():

            while self.state == self.S_RUNNING or self.state == self.S_INIT:

                gangliaUpdate = False

                for s in self.get_services_to_update():
                    ds = self.ganglia.get_datasource_by_cluster_name(s.name)
                    self.logger.debug("Ganglia check returns datasource %s" %
                                      ds)
                    if ds['masterIp'] != s.master:
                        self.logger.debug("Master added to service %s" %
                                          s.name)
                        # not the same master or new one
                        gangliaUpdate = True

                    if s.failed:
                        self.failed_node_action(s)

                    s.needsUpdate = False

                if gangliaUpdate:
                    self.logger.debug("Updating gmetad config")
                    self.update_ganglia()

                sleep(10)

        Thread(target=check).start()

    def failed_node_action(self, service):
        self.logger.debug("Taking action for failed node in service %s" %
                          service.name)
        for node in service.failed:
            if node is service.manager:
                # restart manager and assign it's nodes to the new one
                pass
            elif node is service.master:
                # for mysql it holds the process agent for server
                # need more than simple node restart
                service.restart_node(node)
                service.update_all_mond_agents()
            else:
                service.restart_node(node)

    def get_services_to_update(self):
        '''
            returns services that have to be updated, or ft action is needed
        '''
        return [service for service in self.services if service.needsUpdate]

    #TODO: restart manager, order service to restart agents, replicate data

    def communication(self):
        #TODO: communication between clusters for FT managers
        pass


from conpaas.core.https.client import jsonrpc_get, jsonrpc_post, check_response
from conpaas.core.log import create_logger
from socket import error
from urllib2 import URLError


class Service(Datasource):
    '''
        This class is an abstraction of a ConPaaS service.
    '''

    def __init__(self, name, manager, master=''):
        Datasource.__init__(self, name, manager, master)
        self.ganglia = Ganglia(manager)
        self.agents = []
        self.failed = []
        self.needsUpdate = False
        self.terminate = False
        self.logger = create_logger(__name__)
        self.connect()

    def connect(self):
        '''
            Checks to see when the deployment is completed so we can connect
        '''
        self.logger.debug("Waiting to connect to service %s manager %s" %
                          (self.name, self.manager))
        def wait_for_state():
            """Poll the state of manager till it matches."""
            state = ''
            while state != 'INIT' or state != 'PROLOGUE' or state != 'RUNNING':
                try:
                    state = self.get_manager_state()
                except (error, URLError):
                    sleep(2)

        def check_manager_running():
            #if service started fast we may miss the INIT, or PROLOGUE
            wait_for_state()

            self.ganglia.connect()
            self.__start_master_monitor()
            self.__start_agents_monitor()

        Thread(target=check_manager_running).start()

    def __start_master_monitor(self):
        '''
            Checks service for master to add as backup datasource
        '''
        self.logger.debug("Waiting for new master to service %s manager %s" %
                          (self.name, self.manager))
        def check_master():
            while not self.master:
                hosts = self.ganglia.getCluster(self.name).getHosts()
                if len(hosts) == 2:
                    self.logger.debug("Adding master to datasource cluster %s"
                                      % self.name)
                    self.needsUpdate = True
                    self.master = [host.ip for host in hosts
                                   if host.ip != self.manager][0]
                    sleep(10)    # checking every 10 seconds

        if not self.master:
            Thread(target=check_master).start()

    def __start_agents_monitor(self):
        '''
            Monitors agents to make sure they are properly stop/started
            Updates the agents list, using ganglia.

            The manager and master qualify as agents as well

            TODO: maybe should interact with manager to check for downed nodes
        '''
        self.logger.debug("Started monitoring agent nodes")
        def check_agents():
            while not self.terminate:
                hosts = self.ganglia.getCluster(self.name).getHosts()
# removing manager from list as it's not in the scope of the search
                hosts.remove(self.manager)
                manager_nodes = self.get_manager_node_list()

                new_agents = [host.ip for host in hosts]
                self.logger.debug("Ganglia registered nodes: %s" % new_agents)
                self.logger.debug("Manager registered nodes: %s" %
                                  manager_nodes)
                self.failed = [node for node in self.agents
                               if node not in new_agents]
                self.agents = new_agents

                for node in self.failed:
                    if node not in manager_nodes:
                        # it must mean that it was stopped by manager
                        self.failed.pop(node)

                if not self.needsUpdate and self.failed:
                    self.needsUpdate = True

                sleep(30)    # checking every 30 seconds

        Thread(target=check_agents).start()

    def restart_node(self, node):
        '''
            Orders the manager of the service to restart the failed node.
        '''
        # TODO: check to see on which cloud, and either restart yourself or
        # talk to the coresponding FT manager for restart
        check_response(jsonrpc_post(self.manager, 443, '/', 'restart_node',
                                    {'nodeIp': node}))

    def update_all_mond_agents(self):
        '''
            Updates all the monitoring agents of the nodes
        '''
        self.logger.debug("Updating all monitoring agents on the nodes")
        return check_response(jsonrpc_get(self.manager, 443, '/',
                                          'update_all_gmond'))

    def shutdown(self):
        self.terminate = True

    def get_manager_node_list(self):
        '''
            Talks to manager for checking and ordering things around

            list_nodes and if node not in list and failed it means that it was
            stopped, if node in list and failed we need to take action

            @return L[String] list of ip's registered
        '''
        nodes = check_response(jsonrpc_get(self.manager, 443, '/',
                                           'list_nodes_by_ip'))['nodes']
        self.logger.debug("All nodes registered to service %s" % nodes)
        return nodes

    def get_manager_state(self):
        state = check_response(jsonrpc_get(self.manager, 443, '/',
                                           'get_service_info'))['state']
        self.logger.debug("Service state is %s" % state)
        return state

    @staticmethod
    def from_dict(datasource):
        return Service(datasource['clusterName'],
                       datasource['hostName'],
                       datasource['masterIp'])
