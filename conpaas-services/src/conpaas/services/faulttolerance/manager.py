from conpaas.core.expose import expose
from conpaas.core.ganglia import FaultToleranceGanglia
from conpaas.core.manager import BaseManager
from conpaas.services.xtreemfs.manager.manager import XtreemFSManager


#TODO: register manager to the faulttolerance service
#TODO: ask the director for the address of the faulttolerance
#TODO: if faulttolerance is started after service, add registering
class FaultToleranceManager(XtreemFSManager):

    def __init__(self, config_parser, **kwargs):
        """ Initializes a fault tolerance manager

            @param config_parser: sets up the service

            @param service_cluster: needed for Ganglia
        """
        BaseManager.__init__(self, config_parser, FaultToleranceGanglia)
        self.logger.debug("Entering FaultToleranceManager initialization")
        self.controller.generate_context('faulttolerance')
        #we need minimum configuration for ft
        self.controller.config_clouds({ "mem" : "512", "cpu" : "1" })
        self._init_values()
        self.state = self.S_INIT
        self.logger.debug("Leaving FaultToleranceManager initialization")

    @expose('POST')
    def startup(self, kwargs):
        self.logger.info('FaultToleranceManager starting up')
        return XtreemFSManager.startup(kwargs)

