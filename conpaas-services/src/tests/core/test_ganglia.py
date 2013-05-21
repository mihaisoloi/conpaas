from conpaas.core.ganglia import BaseGanglia, ManagerGanglia, AgentGanglia, \
    FaultToleranceGanglia, Datasource
from tests.conftest import config_parser, simple_config
from mockito import when
from os.path import expanduser


CLUSTER = 'testing-cluster'
CONFIG = config_parser('ec2')
simple_config(CONFIG)
HOME = expanduser('~')
TEST_CONF_FILE = HOME + '/gtest.conf'


def setup_module(module):
    f = open(TEST_CONF_FILE, 'w')
    f.write('')
    f.close()


def teardown_module(module):
    from os import remove
    remove(TEST_CONF_FILE)


class TestManagerGanglia():

    def setup_class(cls):
        when(BaseGanglia).configure().thenReturn(True)
        cls.mg = ManagerGanglia(CONFIG, CLUSTER)
        cls.mg.GMETAD_CONF = TEST_CONF_FILE

    def test_metad_config(self):
        errors = self.mg._metad_config(clusterName=CLUSTER)
        assert not errors


class TestAgentGanglia():

    def setup_class(cls):
        CONFIG.add_section('agent')
        CONFIG.set('agent', 'IP_WHITE_LIST', '127.0.0.1')
        CONFIG.set('agent', 'CONPAAS_HOME', CONFIG.get('manager',
                                                       'CONPAAS_HOME'))
        cls.ag = AgentGanglia(CONFIG, CLUSTER)
        cls.ag.GMOND_CONF = TEST_CONF_FILE

    def test_gmond_conf(self):
        errors = self.ag._mond_config()
        assert not errors

    def test_add_master(self):
        errors = self.ag.add_master('test.ganglia.agent.master')
        assert not errors


class TestFaultToleranceGanglia():

    def setup_class(cls):
        when(BaseGanglia).configure().thenReturn(True)
        cls.ftg = FaultToleranceGanglia(CONFIG, CLUSTER)
        cls.ftg.GMETAD_CONF = TEST_CONF_FILE

    def test_metad_config(self):
        datasources = []
        datasource0 = Datasource(CLUSTER, 'test.ganglia.datasource.host0')
        datasources.append(datasource0.to_dict())
        errors = self.ftg._metad_config(gridName='testGrid',
                                        clusterName=CLUSTER,
                                        datasources=datasources)
        assert not errors

    def test_add_datasources(self):
        datasources = []
        datasource1 = Datasource(CLUSTER, 'test.ganglia.datasource.host1',
                                          'test.ganglia.datasource.master1')
        datasource2 = Datasource(CLUSTER, 'test.ganglia.datasource.host2')
        datasources.append(datasource1.to_dict())
        datasources.append(datasource2.to_dict())
        errors = self.ftg.add_datasources(datasources)
        assert not errors
