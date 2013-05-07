import pytest
import logging
from mock import Mock
from conpaas.services.faulttolerance.manager import FaultToleranceManager
from tests.core.conftest import config_parser


@pytest.fixture(scope='module')
def manager(cloud):
    ''' Params needed for initialisation of the manager '''
    logging.basicConfig()
    if (cloud.get_cloud_type() == 'dummy'):
        return None
    config = config_parser(cloud.get_cloud_type())

    manager = FaultToleranceManager(config)
    mockedManager = Mock(spec=manager)

    return mockedManager


def test_start_service(manager):
    '''Start fault tolerance service'''
    if manager is not None:
        assert manager.startup()
