import pytest
from conpaas.services.faulttolerance.manager import FaultToleranceManager, \
    Service
from tests.conftest import controller, config_parser, simple_config
from conpaas.core.controller import Controller
from mockito import when, unstub
from conpaas.core.https.server import HttpJsonResponse
from conpaas.core.ganglia import Datasource, FaultToleranceGanglia
from conpaas.services.faulttolerance.gmon.ganglia import Ganglia, Cluster, Host


class TestManager():

    @pytest.fixture(scope='module')
    def manager(self, cloud):
        ''' Params needed for initialisation of the manager '''
        if (cloud.get_cloud_type() == 'dummy'):
            return None
        #mocking controller for the manager
        controller(cloud)

        #configuring the parser for the init of the manager
        config = config_parser(cloud.get_cloud_type())
        simple_config(config)

        when(Controller).generate_context().thenReturn(True)
        ftm = FaultToleranceManager(config)
        #we don't need timer for testing
        ftm.controller._Controller__reservation_map['manager'].stop()

        return ftm

    def setup_class(cls):
        when(Ganglia).refresh().thenReturn(True)
        when(FaultToleranceManager).update_ganglia().thenReturn(True)

        when(Service).get_manager_state().thenReturn('INIT')
        when(Service).connect().thenReturn(True)
        cls.test_services = [Service('test1', 'manager1', 'master1'),
                             Service('test2', 'manager2', 'master2'),
                             Service('test3', 'manager3', 'master3')]

        when(FaultToleranceGanglia).add_datasources().thenReturn(True)
        when(FaultToleranceGanglia).\
            get_datasource_by_cluster_name().thenReturn(
                    Service('test1', 'manager1', 'master1').to_dict())

    def test_start_service(self, manager):
        '''Start fault tolerance service'''
        if manager is not None:
            assert manager.startup({'cloud': 'ec2'})

    def test_register(self, manager):
        if manager is not None:
            kwargs = {'datasources': [
                Datasource('testing-cluster',
                           'test.ganglia.datasource.host2').to_dict()]}
            assert isinstance(manager.register(kwargs), HttpJsonResponse)

    def test_classify(self, manager):
        if manager is not None:
            manager.services = self.test_services[:]
            newService = Service('test4', 'manager4', 'master4')
            self.test_services.append(newService)
            assert {0: [], 1: [newService]} == \
                manager.classify(self.test_services)  # adding one service

            assert {0: self.test_services[:len(self.test_services)-1], 1: []} == \
                manager.classify([self.test_services.pop()])  # removing 3 services

            assert len(manager.services) == 1

    def test_get_services_to_update(self, manager):
        if manager is not None:
            manager.services = self.test_services[:]

            assert not manager.get_services_to_update()

            manager.services[0].needsUpdate = True

            assert len(manager.get_services_to_update()) == 1

            manager.services[0].needsUpdate = False

    @pytest.mark.skipif("True")
    def test_check_for_updates(self, manager):
        if manager is not None:
            manager.services = self.test_services[:]

            manager.services[0].master = 'master10'
            manager.services[0].needsUpdate = True

            assert not manager.services[0].needsUpdate

            #need to stop threads
            manager.state = manager.S_STOPPED
            #manager.services[0].needsUpdate = False

    def teardown_class(cls):
        unstub()


@pytest.mark.slow
class TestService():

    def setup_class(cls):
        cls.manager = '192.168.1.1'
        cls.master = '192.168.1.2'
        cluster_name = 'test_service'

        when(Ganglia).refresh().thenReturn(True)
        when(Service).get_manager_state().thenReturn('INIT')
        when(Service).get_manager_node_list().thenReturn([cls.manager,
                                                          cls.master])

        cls.cluster = Cluster(cluster_name, "mihai", 12, "", "")
        cls.cluster.addHost(Host('host_manager', cls.manager, 1, 1, 1))
        when(Ganglia).getCluster().thenReturn(cls.cluster)

        cls.service = Service(cluster_name, cls.manager)

    def test_connect(self):
        import time
        assert not self.service.needsUpdate
        self.cluster.addHost(Host('host_master', self.master, 2, 2, 2))
        time.sleep(10)
        assert self.service.master == self.master
        assert self.manager in self.service.agents
        assert self.service.needsUpdate

    def teardown_class(cls):
        unstub()
        cls.service.shutdown()
