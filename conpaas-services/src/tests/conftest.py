from conpaas.core.clouds import dummy, ec2, opennebula
import pytest
from ConfigParser import ConfigParser
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.compute.drivers.ec2 import EC2NodeDriver
from libcloud.compute.drivers.opennebula import OpenNebulaNodeDriver
from mock import Mock
from libcloud.compute.base import Node
from libcloud.compute.types import NodeState
from conpaas.core.controller import Controller
from os.path import expanduser


cloud_names = ["dummy", "ec2", "opennebula"]
#TODO: cloud_name should be actual name
cloud_name = 'iaas'
PROJECT_HOME = expanduser('~')+'/workspace/conpaas/'


def __generate_nodes(interval=[]):
    return [
        Node(i, NodeState.RUNNING, 'node%i' % i, '127.0.0.%i' % i,
             '192.168.1.%i' % i, None) for i in range(*interval)]


@pytest.fixture(scope="module")
def mocked_driver():
    '''#mocking the driver for testing purposes'''
    attrs = {'list_nodes.return_value': __generate_nodes([1, 3]),
             'create_node.return_value': __generate_nodes([3, 5]),
             'list_sizes.return_value': [
             #node sizes of the Provider
             #EC2 uses size_id, OpenNebula's instance_type is the name
             #NodeSize has too many arguments
             type("NodeSize", (), {'id': 'm1.small', 'name': 'small'}),
             type("NodeSize", (), {'id': 'm1.medium', 'name': 'medium'}),
             type("NodeSize", (), {'id': 't1.micro', 'name': 'custom'}),
             type("NodeSize", (), {'id': 'm1.large', 'name': 'large'})]}
    return Mock(**attrs)


@pytest.fixture(scope='module')
def controller(cloud):
    ''' Params needed for initialisation of the controller '''
    if (cloud.get_cloud_type() == 'dummy'):
        return None
    conf = config_parser(cloud.get_cloud_type())
    simple_config(conf)
    controller = Controller(conf)
    #we don't need timer for testing
    controller._Controller__reservation_map['manager'].stop()
    mockedController = Mock(spec=controller)
    #for testing purposes
    mockedController.deduct_credit.return_value = True
    mockedController._Controller__default_cloud = cloud
    mockedController._Controller__wait_for_nodes.return_value = \
        (mockedController._Controller__default_cloud.driver.list_nodes(), [])
    mockedController.generate_context.return_value = True
    return mockedController


def config_parser(name):
    iaas_config = ConfigParser()

    def __check_params(params):
        for field in params:
            assert iaas_config.has_option(cloud_name, field)
            assert iaas_config.get(cloud_name, field) != ''

    if name == cloud_names[0]:
        iaas_config.add_section('iaas')
        iaas_config.set('iaas', 'DRIVER', 'DUMMY')
    elif name == cloud_names[1]:
        ec2_params = ['USER', 'PASSWORD',
                      'IMAGE_ID', 'SIZE_ID',
                      'SECURITY_GROUP_NAME',
                      'KEY_NAME', 'REGION']
        iaas_config.read(PROJECT_HOME +
                         "conpaas-services/config/cloud/ec2.cfg.example")
        __check_params(ec2_params)
    elif name == cloud_names[2]:
        nebula_params = ['URL', 'USER', 'PASSWORD', 'IMAGE_ID',
                         'INST_TYPE', 'NET_ID', 'NET_GATEWAY',
                         'NET_NAMESERVER', 'OS_ARCH', 'OS_ROOT',
                         'DISK_TARGET', 'CONTEXT_TARGET']
        iaas_config.read(PROJECT_HOME +
                         "conpaas-services/config/cloud/opennebula.cfg.example")
        __check_params(nebula_params)
    return iaas_config


def simple_config(config_parser):
    ''' adding manager section for Controller '''
    config_parser.add_section('manager')
    config_parser.set('manager', 'TYPE', 'helloworld')
    config_parser.set('manager', 'SERVICE_ID', '1')
    config_parser.set('manager', 'USER_ID', '123')
    config_parser.set('manager', 'CREDIT_URL',
                      'https://localhost:5555/credit')
    config_parser.set('manager', 'TERMINATE_URL',
                      'https://localhost:5555/terminate')
    config_parser.set('manager', 'CA_URL',
                      'https://localhost:5555/ca')
    config_parser.set('manager', 'APP_ID', '1')
    config_parser.set('manager', 'LOG_FILE', '~/manager.log')
    config_parser.set('manager', 'CONPAAS_HOME', 'conpaas-services')


@pytest.fixture(scope="module", params=cloud_names)
def cloud(request, mocked_driver):
    """instantiates clouds"""
    name = request.param

    def __connect_and_check(cloud):
        cloud._connect()
        assert cloud.connected

    if name == cloud_names[0]:
        dums = dummy.DummyCloud(name+'-test', None)
        __connect_and_check(dums)
        assert isinstance(dums.driver, get_driver(Provider.DUMMY))
        return dums
    elif name == cloud_names[1]:
        aws_ec2 = ec2.EC2Cloud(cloud_name, config_parser(name))
        aws_ec2.set_context_template('')
        __connect_and_check(aws_ec2)
        mocked_driver.mock_add_spec(EC2NodeDriver, True)
        assert isinstance(aws_ec2.driver,
                          get_driver(Provider.EC2_US_WEST_OREGON))
        aws_ec2.driver = mocked_driver
        return aws_ec2
    elif name == cloud_names[2]:
        nebula = opennebula.OpenNebulaCloud(cloud_name, config_parser(name))
        nebula.set_context_template('')
        __connect_and_check(nebula)
        mocked_driver.mock_add_spec(OpenNebulaNodeDriver, True)
        assert isinstance(nebula.driver, get_driver(Provider.OPENNEBULA))
        #let's us iterate over the nodes
        mocked_driver.create_node.side_effect = __generate_nodes([3, 5])
        nebula.driver = mocked_driver
        return nebula
