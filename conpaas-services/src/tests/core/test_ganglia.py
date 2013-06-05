from conpaas.core.ganglia import BaseGanglia, ManagerGanglia, AgentGanglia, \
    FaultToleranceGanglia
from conpaas.services.faulttolerance.manager import Service
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


#def teardown_module(module):
#    from os import remove
#    remove(TEST_CONF_FILE)


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
        when(Service).connect().thenReturn(True)
        cls.ftg = FaultToleranceGanglia(CONFIG, CLUSTER)
        cls.ftg.GMETAD_CONF = TEST_CONF_FILE
        cls.datasources = {}

    def build_datasource(self, cluster_name, manager, master=''):
        datasource = Service(cluster_name, manager, master).to_dict()
        self.datasources[cluster_name] = datasource

    def test_metad_config(self):
        self.build_datasource(CLUSTER + '0', 'test.ganglia.datasource.host0')
        errors = self.ftg._metad_config(gridName='testGrid',
                                        clusterName=CLUSTER,
                                        datasources=self.datasources.values())
        assert not errors

    def test_add_datasources(self):
        self.build_datasource(CLUSTER + '1', 'test.ganglia.datasource.host1',
                                             'test.ganglia.datasource.master1')
        self.build_datasource(CLUSTER + '2', 'test.ganglia.datasource.host2')
        errors = self.ftg.add_datasources(self.datasources.values())
        assert not errors

    def test_get_datasource_by_cluster_name(self):
        ds = self.ftg.get_datasource_by_cluster_name(CLUSTER + '1')
        assert self.datasources[CLUSTER + '1'] == ds
