from conpaas.core.expose import expose
from conpaas.core.ganglia import FaultToleranceGanglia, Datasource
from conpaas.core.manager import BaseManager
from conpaas.services.xtreemfs.manager.manager import XtreemFSManager
from conpaas.core.https.server import HttpJsonResponse
from gmon.ganglia import Ganglia


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
        datasources = kwargs["datasources"]
        self.ganglia.add_datasources(datasources)
        self.ganglia.restart()
        return HttpJsonResponse()

    def update(self, serviceUpdate):
        '''
            Deciding if a service was removed, or added:
            if removed -> interrupt monitoring, and clean it's history
            if added -> watch for master creation

            @return D{0: L[removed], 1: L[added]}
        '''
        names = lambda y: [x.name for x in y]
        func = lambda x,y: [s for s in x if s.name not in names(y)]

        added = func(serviceUpdate, self.services)

        for newService in added:
            newService.start_checking_master()

        removed = func(self.services, serviceUpdate)

        self.services.extend(added)
        self.services = func(self.services, removed)

        return {k: v for (k, v) in enumerate([removed, added])}

    def datasource_to_service(self, datasources):
        return [Service.from_dict(datasource)
                for datasource in datasources]

    #TODO: restart manager, order service to restart agents, replicate data

    def communication(self):
        #TODO: communication between clusters for FT managers
        pass


class Service(Datasource):
    '''
        This class is an abstraction of a ConPaaS service.
    '''

    def __init__(self, name, manager, master=None):
        Datasource.__init__(self, name, manager, master)
        self.ganglia = Ganglia(manager)
        self.ganglia.connect()
        self.agents = []
        self.needsUpdate = False

    def start_checking_master(self):
        '''
            Checks service for master to add as backup datasource
        '''
        from threading import Thread
        def check_master():
            while self.master is None:
                hosts = self.ganglia.getCluster().getHosts()
                if len(hosts) == 2:
                    self.needsUpdate = True
                    self.master = hosts[2]

        if self.master is None:
            Thread(target=check_master).start()

    def monitor_agents(self):
        '''
            Monitors agents to make sure they are properly stop/started
            Updates the agents list, using ganglia.
        '''
        pass

    def manager_communication(self):
        '''
            Talks to manager for checking and ordering things around

            list_nodes and if node not in list and failed it means that it was
            stopped, if node in list and failed we need to take action
        '''
        from threading import Thread

        pass

    @staticmethod
    def from_dict(datasource):
        return Service(datasource['clusterName'],
                       datasource['hostName'],
                       datasource['masterIp'])
