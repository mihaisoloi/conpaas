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


class Service(Datasource):
    '''
        This class is an abstraction of a ConPaaS service.
    '''

    def __init__(self, name, manager, master=None):
        Datasource.__init__(self, name, manager, master)
        self.ganglia = Ganglia(manager)
        self.ganglia.connect()
        self.agents = []

    def start_checking_master(self, ganglia_client):
        '''
            Checks service for master to add as backup datasource
        '''
        hosts = ganglia_client.getCluster().getHosts()
        if len(hosts) == 2:
            self.register()

    @staticmethod
    def from_dict(datasource):
        return Service(datasource['clusterName'],
                       datasource['hostName'],
                       datasource['masterIp'])
