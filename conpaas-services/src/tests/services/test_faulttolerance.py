import pytest
import json
from conpaas.services.faulttolerance.manager import FaultToleranceManager
from tests.conftest import controller, config_parser, simple_config
from conpaas.core.controller import Controller
from mockito import when
from conpaas.core.https.server import HttpJsonResponse
from conpaas.core.ganglia import Datasource, FaultToleranceGanglia


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
        kwargs = {'datasources': [
            Datasource('testing-cluster',
                       'test.ganglia.datasource.host2').to_dict()]}
        assert isinstance(manager.register(json.dumps(kwargs)),
                          HttpJsonResponse)
