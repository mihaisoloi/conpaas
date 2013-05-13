import pytest
from mock import Mock
from conpaas.services.faulttolerance.manager import FaultToleranceManager
from tests import conftest


@pytest.fixture(scope='module')
def manager(cloud):
    ''' Params needed for initialisation of the manager '''
    if (cloud.get_cloud_type() == 'dummy'):
        return None
    mockedController = conftest.controller(cloud)

    #configuring the parser for the init of the manager
    config_parser = conftest.config_parser(cloud.get_cloud_type())
    conftest.simple_config(config_parser)

    ftm = FaultToleranceManager(config_parser)
    #we don't need timer for testing
    ftm.controller._Controller__reservation_map['manager'].stop()
    mockedManager = Mock(spec=ftm)
    mockedManager.controller = mockedController

    return mockedManager


def test_start_service(manager):
    '''Start fault tolerance service'''
    if manager is not None:
        assert manager.startup()
