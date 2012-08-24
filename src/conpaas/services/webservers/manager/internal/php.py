'''
Copyright (c) 2010-2012, Contrail consortium.
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
 3. Neither the name of the <ORGANIZATION> nor the
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


Created on Jul 11, 2011

@author: ielhelw
'''

from threading import Thread
import re, tarfile, tempfile, stat, os.path
import pickle, sys

from conpaas.services.webservers.manager.config import CodeVersion, PHPServiceConfiguration
from conpaas.services.webservers.agent import client
from conpaas.core.http import HttpErrorResponse, HttpJsonResponse

from . import BasicWebserversManager, ManagerException
from conpaas.core.expose import expose

from conpaas.core import git

class PHPManager(BasicWebserversManager):
  
    def __init__(self, config_parser, **kwargs):
      BasicWebserversManager.__init__(self, config_parser)
      print kwargs['reset_config']
      print kwargs['state']
      print kwargs['crt_state']

      sys.stdout.flush()
      if kwargs['reset_config']:
          self._create_initial_configuration()

      ##### FT_start
      if kwargs['state'] != None:
          pickle_path = kwargs['state'] 
          f = open(pickle_path, 'r')
          config = pickle.load(f)
          f.close()
          self._configuration_set(config)
          f = open(kwargs['crt_state'], 'r')
          crt_state = f.readline().rstrip('\r\n')
	  role_id = f.readline().rstrip('\r\n')
          f.close()
	  self.memcache.set(self.ROLE_ID, role_id)
          self._state_set(crt_state)
      ##### FT_stop
      self._register_scalaris(kwargs['scalaris'])
  
    def _update_code(self, config, nodes):
      for serviceNode in nodes:
        # Push the current code version via GIT if necessary
        if config.codeVersions[config.currentCodeVersion].type == 'git':
          _, err = git.git_push(git.DEFAULT_CODE_REPO, serviceNode.ip)
          if err:
            self.logger.debug('git-push to %s: %s' % (serviceNode.ip, err))
        
        try:
          client.updatePHPCode(serviceNode.ip, 5555, config.currentCodeVersion, config.codeVersions[config.currentCodeVersion].type, os.path.join(self.code_repo, config.currentCodeVersion))
        except client.AgentException:
          self.logger.exception('Failed to update code at node %s' % str(serviceNode))
          self._state_set(self.S_ERROR, msg='Failed to update code at node %s' % str(serviceNode))
          raise
  
    def _start_proxy(self, config, nodes, role_id=None):
      kwargs = {
                'web_list': config.getWebTuples(),
                'fpm_list': config.getBackendTuples(),
                'cdn': config.cdn
                }
    
      for proxyNode in nodes:
        if role_id == None:
          _role_id = self._get_role_id()
        else:
          _role_id = role_id
        kwargs['role_id'] = _role_id
        try:
          if config.currentCodeVersion != None:
            client.createHttpProxy(proxyNode.ip, 5555,
                                   config.proxy_config.port,
                                   config.currentCodeVersion,
                                   **kwargs)
        except client.AgentException:
          self.logger.exception('Failed to start proxy at node %s' % str(proxyNode))
          self._state_set(self.S_ERROR, msg='Failed to start proxy at node %s' % str(proxyNode))
          raise
  
    def _update_proxy(self, config, nodes):
      kwargs = {
                'web_list': config.getWebTuples(),
                'fpm_list': config.getBackendTuples(),
                'cdn': config.cdn
                }
    
      for proxyNode in nodes:
        try:
          if config.currentCodeVersion != None:
            client.updateHttpProxy(proxyNode.ip, 5555,
                                   config.proxy_config.port,
                                   config.currentCodeVersion,
                                   **kwargs)
        except client.AgentException:
          self.logger.exception('Failed to update proxy at node %s' % str(proxyNode))
          self._state_set(self.S_ERROR, msg='Failed to update proxy at node %s' % str(proxyNode))
          raise
  
    def _start_backend(self, config, nodes, role_id=None):
      for serviceNode in nodes:
        if role_id == None:
          _role_id = self._get_role_id()
        else:
          _role_id = role_id
        try:
          client.createPHP(serviceNode.ip, 5555, config.backend_config.port, config.backend_config.scalaris, config.backend_config.php_conf.conf, _role_id)
        except client.AgentException:
          self.logger.exception('Failed to start php at node %s' % str(serviceNode))
          self._state_set(self.S_ERROR, msg='Failed to start php at node %s' % str(serviceNode))
          raise
  
    def _update_backend(self, config, nodes):
      for serviceNode in nodes:
        try: client.updatePHP(serviceNode.ip, 5555, config.backend_config.port, config.backend_config.scalaris, config.backend_config.php_conf.conf)
        except client.AgentException:
          self.logger.exception('Failed to update php at node %s' % str(serviceNode))
          self._state_set(self.S_ERROR, msg='Failed to update php at node %s' % str(serviceNode))
          raise
  
    def _stop_backend(self, config, nodes):
      for serviceNode in nodes:
        try: client.stopPHP(serviceNode.ip, 5555)
        except client.AgentException:
          self.logger.exception('Failed to stop php at node %s' % str(serviceNode))
          self._state_set(self.S_ERROR, msg='Failed to stop php at node %s' % str(serviceNode))
          raise
  
    @expose('GET')
    def get_service_info(self, kwargs):
      if len(kwargs) != 0:
        return HttpErrorResponse(ManagerException(ManagerException.E_ARGS_UNEXPECTED, kwargs.keys()).message)
      return HttpJsonResponse({'state': self._state_get(), 'type': 'PHP'})

    @expose('GET')
    def get_configuration(self, kwargs):
      if len(kwargs) != 0:
        return HttpErrorResponse(ManagerException(ManagerException.E_ARGS_UNEXPECTED, kwargs.keys()).message)
      config = self._configuration_get()
      phpconf = {}
      for key in config.backend_config.php_conf.defaults:
        if key in config.backend_config.php_conf.conf:
          phpconf[key] = config.backend_config.php_conf.conf[key]
        else:
          phpconf[key] = config.backend_config.php_conf.defaults[key]
      return HttpJsonResponse({
              'codeVersionId': config.currentCodeVersion,
              'phpconf': phpconf,
              'cdn': config.cdn,
              })

    @expose('POST')
    def update_php_configuration(self, kwargs):
      codeVersionId = None
      if 'codeVersionId' in kwargs:
        codeVersionId = kwargs.pop('codeVersionId')
      config = self._configuration_get()
      phpconf = {}
      if 'phpconf' in kwargs:
        phpconf = kwargs.pop('phpconf')
        for key in phpconf.keys():
          if key not in config.backend_config.php_conf.defaults:
            return HttpErrorResponse(ManagerException(ManagerException.E_ARGS_UNEXPECTED, 'phpconf attribute "%s"' % (str(key))).message)
          if not re.match(config.backend_config.php_conf.format[key], phpconf[key]):
            return HttpErrorResponse(ManagerException(ManagerException.E_ARGS_INVALID).message)
    
      if len(kwargs) != 0:
        return HttpErrorResponse(ManagerException(ManagerException.E_ARGS_UNEXPECTED, kwargs.keys()).message)
    
      if codeVersionId == None and  not phpconf:
        return HttpErrorResponse(ManagerException(ManagerException.E_ARGS_MISSING, 'at least one of "codeVersionId" or "phpconf"').message)
    
      if codeVersionId and codeVersionId not in config.codeVersions:
        return HttpErrorResponse(ManagerException(ManagerException.E_ARGS_INVALID, detail='Invalid codeVersionId').message)
    
      dstate = self._state_get()
      if dstate == self.S_INIT or dstate == self.S_STOPPED:
        if codeVersionId: config.currentCodeVersion = codeVersionId
        for key in phpconf:
          config.backend_config.php_conf.conf[key] = phpconf[key]
        self._configuration_set(config)
      elif dstate == self.S_RUNNING:
        self._state_set(self.S_ADAPTING, msg='Updating configuration')
        Thread(target=self.do_update_configuration, args=[config, codeVersionId, phpconf]).start()
      else:
        return HttpErrorResponse(ManagerException(ManagerException.E_STATE_ERROR).message)
      return HttpJsonResponse()

    def do_update_configuration(self, config, codeVersionId, phpconf):
      if phpconf:
        for key in phpconf:
          config.backend_config.php_conf.conf[key] = phpconf[key]
        self._update_backend(config, config.getBackendServiceNodes())
      if codeVersionId != None:
        self.prevCodeVersion = config.currentCodeVersion
        config.currentCodeVersion = codeVersionId
        self._update_code(config, config.serviceNodes.values())
        self._update_web(config, config.getWebServiceNodes())
        self._update_proxy(config, config.getProxyServiceNodes())
      self._state_set(self.S_RUNNING)
      self._configuration_set(config)
  
    def _create_initial_configuration(self):
      print 'CREATING INIT CONFIG'
      config = PHPServiceConfiguration()
      config.backend_count = 0
      config.web_count = 0
      config.proxy_count = 1
      config.cdn = False
    
      if not os.path.exists(self.code_repo):
        os.makedirs(self.code_repo)
    
      fileno, path = tempfile.mkstemp()
      fd = os.fdopen(fileno, 'w')
      fd.write('''<html>
    <head>
    <title>Welcome to ConPaaS!</title>
    </head>
    <body bgcolor="white" text="black">
    <center><h1>Welcome to ConPaaS!</h1></center>
    </body>
    </html>''')
      fd.close()
      os.chmod(path, stat.S_IRWXU | stat.S_IROTH | stat.S_IXOTH)
    
      if len(config.codeVersions) > 0: return
      tfile = tarfile.TarFile(name=os.path.join(self.code_repo,'code-default'), mode='w')
      tfile.add(path, 'index.html')
      tfile.close()
      os.remove(path)
      config.codeVersions['code-default'] = CodeVersion('code-default', 'code-default.tar', 'tar', description='Initial version')
      config.currentCodeVersion = 'code-default'
      self._configuration_set(config)
      self._state_set(self.S_INIT)
  
    def _register_scalaris(self, scalaris):
      config = self._configuration_get()
      config.backend_config.scalaris = scalaris
      self._configuration_set(config)

    @expose('POST')
    def cdn_enable(self, params):
        '''
        Enable/disable CDN offloading.
        The changes must be reflected on the load balancer a.k.a proxy
        '''
        try:
            enable = params['enable']
            if enable:
                cdn = params['address']
                self.logger.info('Enabling CDN hosted at "%s"' %(cdn))
            else:
                cdn = False
                self.logger.info('Disabling CDN')
            config = self._configuration_get()
            config.cdn = cdn
            self._update_proxy(config, config.getProxyServiceNodes())
            self._configuration_set(config)
            return HttpJsonResponse({'cdn': config.cdn})
        except Exception as e:
            self.logger.exception(e)
            return HttpErrorResponse(str(e))

