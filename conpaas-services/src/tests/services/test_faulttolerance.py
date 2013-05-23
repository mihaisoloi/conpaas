import pytest
from conpaas.services.faulttolerance.manager import FaultToleranceManager, \
                                                    Service
from tests.conftest import controller, config_parser, simple_config
from conpaas.core.controller import Controller
from mockito import when, unstub
from conpaas.core.https.server import HttpJsonResponse
from conpaas.core.ganglia import Datasource, FaultToleranceGanglia
from conpaas.services.faulttolerance.gmon.ganglia import Ganglia


@pytest.fixture(scope='module')
def manager(cloud):
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


def test_start_service(manager):
    '''Start fault tolerance service'''
    if manager is not None:
        assert manager.startup({'cloud':'ec2'})


def test_register(manager):
    if manager is not None:
        when(FaultToleranceGanglia).add_datasources().thenReturn(True)
        when(Ganglia).connect().thenReturn(True)
        kwargs = {'datasources': [
            Datasource('testing-cluster',
                       'test.ganglia.datasource.host2').to_dict()]}
        assert isinstance(manager.register(kwargs), HttpJsonResponse)


def test_update(manager):
    if manager is not None:
        when(Ganglia).connect().thenReturn(True)
        when(Service).start_checking_master().thenReturn(True)
        test_services = [Service('test1','manager1','master1'),
                         Service('test2','manager2','master2'),
                         Service('test3','manager3','master3')]
        manager.services = test_services[:]
        newService = Service('test4', 'manager4', 'master4')
        test_services.append(newService)
        assert {0: [], 1: [newService]} == \
               manager.update(test_services) # adding one service

        assert {0: test_services[:len(test_services)-1], 1: []} == \
               manager.update([test_services.pop()]) #removing 3 services

        assert len(manager.services) == 1



