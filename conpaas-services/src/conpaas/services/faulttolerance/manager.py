from conpaas.core.expose import expose
from conpaas.core.ganglia import FaultToleranceGanglia, Datasource
from conpaas.core.manager import BaseManager
from conpaas.services.xtreemfs.manager.manager import XtreemFSManager


#TODO: register manager to the faulttolerance service
#TODO: if faulttolerance is started after service, add registering received from Director
class FaultToleranceManager(XtreemFSManager):

    def __init__(self, config_parser, **kwargs):
        """ Initializes a fault tolerance manager

            @param config_parser: sets up the service

            @param service_cluster: needed for Ganglia
        """

        BaseManager.__init__(self, config_parser, FaultToleranceGanglia)

        self.logger.debug("Entering FaultToleranceManager initialization")
        #we are using the same contextualization as xtreemfs
        #self.controller.generate_context('xtreemfs')
        #we need minimum configuration for ft
        self.controller.config_clouds({ "mem" : "512", "cpu" : "1" })
        self._init_values()
        self.state = self.S_INIT
        self.logger.debug("Leaving FaultToleranceManager initialization")

    @expose('POST')
    def startup(self, kwargs):
        self.logger.info('FaultToleranceManager starting up')
        return XtreemFSManager.startup(kwargs)

    @expose('POST')
    def register(self, services):
        '''
            Registering services to the faulttolerance service

            @param services: all the services that must be registered
            @type services: L{conpaas.core.node.ServiceNode}
        '''

        datasources = []
        for service in services:
            datasources.append(Datasource('%s-u%s-s%s' % (service.type,
                                                          service.user_id,
                                                          service.sid),
                               service.ip))
        self.ganglia.add_datasources(datasources)
