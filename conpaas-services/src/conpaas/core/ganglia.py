"""
Copyright (c) 2010-2013, Contrail consortium.
All rights reserved.

Redistribution and use in source and binary forms,
with or without modification, are permitted provided
that the following conditions are met:

 1. Redistributions of source code must retain the
    above copyright notice, this list of conditions
    and the following disclaimer.
 2. Redistributions in binary form must reproduce
    the above copyright notice, this list of
    conditions and the following disclaimer in the
    documentation and/or other materials provided
    with the distribution.
 3. Neither the name of the Contrail consortium nor the
    names of its contributors may be used to endorse
    or promote products derived from this software
    without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
"""

import os
from shutil import copyfile, copy
from Cheetah.Template import Template
from conpaas.core.misc import run_cmd


class BaseGanglia(object):
    """Basic Ganglia configuration and startup. Valid for both managers and
    agents. Not to be used directly!"""

    GANGLIA_ETC = '/etc/ganglia'
    GANGLIA_CONFD = os.path.join(GANGLIA_ETC, 'conf.d')
    GMOND_CONF = os.path.join(GANGLIA_ETC, 'gmond.conf')
    GANGLIA_MODULES_DIR = '/usr/lib/ganglia/python_modules/'

    def __init__(self, service_cluster):
        """Set basic values"""
        self.cluster_name = service_cluster
        self.setuid = 'no'
        self.host_dmax = 300

        # Set by subclasses
        self.manager_ip = None
        self.masterIp = None
        self.cps_home = None

    def configure(self):
        """Create Ganglia configuration. Gmond is needed by managers and
        agents."""
        if not os.path.isdir(self.GANGLIA_CONFD):
            os.mkdir(self.GANGLIA_CONFD)
        if not os.path.isdir(self.GANGLIA_MODULES_DIR):
            os.mkdir(self.GANGLIA_MODULES_DIR)

        # Copy modpython.conf
        src = os.path.join(self.cps_home, 'contrib', 'ganglia_modules',
                           'modpython.conf')
        copy(src, self.GANGLIA_CONFD)

        self._mond_config()

    def _mond_config(self):
        ''' Write gmond.conf '''
        values = {
            'clusterName': self.cluster_name, 'setuid': self.setuid,
            'hostdmax': self.host_dmax, 'managerIp': self.manager_ip,
            'masterIp': self.masterIp
        }
        src = open(os.path.join(self.cps_home, 'config', 'ganglia',
                                'ganglia-gmond.tmpl')).read()
        open(self.GMOND_CONF, 'w').write(str(Template(src, values)))

    def add_master(self, masterIp):
        self.masterIp = masterIp
        self._mond_config()
        self.restart()

    def add_modules(self, modules):
        """Install additional modules and restart ganglia-monitor"""
        for module in modules:
            # Copy conf files into ganglia conf.d
            filename = os.path.join(self.cps_home, 'contrib',
                                    'ganglia_modules', module + '.pyconf')

            copy(filename, os.path.join(self.GANGLIA_CONFD, module + '.conf'))

            # Copy python modules
            filename = os.path.join(self.cps_home, 'contrib',
                                    'ganglia_modules', module + '.py')
            copy(filename, self.GANGLIA_MODULES_DIR)

        self.restart()

    def start(self):
        """Services startup"""
        _, err = run_cmd('/etc/init.d/ganglia-monitor start')
        if err:
            return 'Error starting ganglia-monitor: %s' % err

    def restart(self):
        """Upon service addition to the mond file we need to restart gmond"""

        _, err = run_cmd('/etc/init.d/ganglia-monitor restart')
        if err:
            return 'Error restarting gmond: %s' % err


class ManagerGanglia(BaseGanglia):

    GMETAD_CONF = '/etc/ganglia/gmetad.conf'

    def __init__(self, config_parser, service_cluster):
        """Same as for the base case, but with localhost as manager_ip"""
        BaseGanglia.__init__(self, service_cluster)

        self.manager_ip = config_parser.get('manager', 'MY_IP')
        self.cps_home = config_parser.get('manager', 'CONPAAS_HOME')

    def _metad_config(self, gridName=None, clusterName=None,
                      datasources=None):
        """ Configure ganglia gmetad.conf """
        src = open(os.path.join(self.cps_home, 'config', 'ganglia',
                   'ganglia-gmetad.tmpl')).read()
        tmpl = Template(src, {'gridName': gridName,
                              'clusterName': clusterName,
                              'datasources': datasources})

        open(self.GMETAD_CONF, 'w').write(str(tmpl))

    def _fe_config(self):
        """ Frontend configuration"""
        if not os.path.isdir('/var/www'):
            os.mkdir('/var/www')
        run_cmd('cp -a \
                 /root/ConPaaS/contrib/ganglia_frontend/ganglia /var/www')

        copy(os.path.join(self.cps_home, 'contrib', 'ganglia_modules',
             'nginx-manager.conf'), '/var/cache/cpsagent')

        copy('/etc/nginx/fastcgi_params', '/var/cache/cpsagent/')

        copy(os.path.join(self.cps_home, 'contrib', 'ganglia_modules',
             'www.conf'), '/etc/php5/fpm/pool.d/')

        copyfile(os.path.join(self.cps_home, 'config', 'ganglia',
                              'ganglia_frontend.tmpl'),
                 '/etc/nginx/nginx.conf')

    def configure(self):
        """Here we also need to configure gmetad and the ganglia frontend"""
        BaseGanglia.configure(self)

        self._metad_config(clusterName=self.cluster_name)
        self._fe_config()

    def start(self):
        """We also need to start gmetad, php5-fpm and nginx"""
        err = BaseGanglia.start(self)
        if err:
            return err

        cmds = ('/etc/init.d/gmetad start',
                '/etc/init.d/php5-fpm start',
                '/usr/sbin/nginx -c /var/cache/cpsagent/nginx-manager.conf')

        for cmd in cmds:
            _, err = run_cmd(cmd)
            if err:
                return "Error executing '%s': %s" % (cmd, err)


class AgentGanglia(BaseGanglia):

    def __init__(self, config_parser, service_cluster):
        """Same as for the base case, but with proper manager_ip"""
        BaseGanglia.__init__(self, service_cluster)

        self.manager_ip = config_parser.get('agent', 'IP_WHITE_LIST')
        self.cps_home = config_parser.get('agent', 'CONPAAS_HOME')


class FaultToleranceGanglia(ManagerGanglia):

    def __init__(self, config_parser, service_cluster,
                 gridName='faulttolerance'):
        ManagerGanglia.__init__(self, config_parser, service_cluster)
        self.gridName = gridName

    def configure(self):
        """Configuring the FT gmetad and the ganglia communication"""
        BaseGanglia.configure(self)
        self._metad_config(self.gridName, self.cluster_name)
        self._fe_config()

    def add_datasources(self, datasources):
        """ Add aditional datasources for the already started services plus the
            one for the newly started services, we must ensure that all values
            are in the template file

            @param datasources: list of Datasource objects
            @type datasources: L{conpaas.core.ganglia.Datasource}
        """
        self._metad_config(self.gridName, self.cluster_name, datasources)

    def restart(self):
        """Upon service addition to the metad file we need to restart gmetad"""

        _, err = run_cmd('/etc/init.d/gmetad restart')
        if err:
            return 'Error restarting gmetad: %s' % err


class Datasource(object):

    def __init__(self, clusterName, hostName, masterIp=None):
        self.name = clusterName
        self.manager = hostName
        self.master = masterIp

    def to_dict(self):
        return {'clusterName': self.name,
                'hostName': self.manager,
                'masterIp': self.master}
