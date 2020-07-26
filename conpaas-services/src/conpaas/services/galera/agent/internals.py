# -*- coding: utf-8 -*-

"""
    :copyright: (C) 2010-2013 by Contrail Consortium.
"""

from os.path import exists, join
from os import remove
from threading import Lock
import pickle

#from conpaas.core.misc import get_ip_address
from conpaas.core.agent import BaseAgent, AgentException
from conpaas.services.galera.agent import role 

from conpaas.core.https.server import HttpErrorResponse, HttpJsonResponse, FileUploadField
from conpaas.core.expose import expose

class GaleraAgent(BaseAgent):

    def __init__(self, config_parser):
      BaseAgent.__init__(self, config_parser)
      self.config_parser = config_parser

      self.my_ip = config_parser.get('agent', 'MY_IP')
      self.VAR_TMP = config_parser.get('agent', 'VAR_TMP')
      self.VAR_CACHE = config_parser.get('agent', 'VAR_CACHE')
      self.VAR_RUN = config_parser.get('agent', 'VAR_RUN')

      self.main_file = join(self.VAR_TMP, 'main.pickle')
      self.subordinate_file = join(self.VAR_TMP, 'subordinate.pickle')
     
      self.main_lock = Lock()
      self.subordinate_lock = Lock()
     
    def _get(self, get_params, class_file, pClass):
        if not exists(class_file):
            return HttpErrorResponse(AgentException(
                AgentException.E_CONFIG_NOT_EXIST).message)
        try:
            fd = open(class_file, 'r')
            p = pickle.load(fd)
            fd.close()
        except Exception as e:
            ex = AgentException(AgentException.E_CONFIG_READ_FAILED, 
                pClass.__name__, class_file, detail=e)
            self.logger.exception(ex.message)
            return HttpErrorResponse(ex.message)
        else:
            return HttpJsonResponse({'return': p.status()})

    def _create(self, post_params, class_file, pClass):
        if exists(class_file):
            return HttpErrorResponse(AgentException(
                AgentException.E_CONFIG_EXISTS).message)
        try:
            if type(post_params) != dict:
                raise TypeError()
            self.logger.debug('Creating class')
            p = pClass(**post_params)
            self.logger.debug('Created class')
        except (ValueError, TypeError) as e:
            ex = AgentException(AgentException.E_ARGS_INVALID, detail=str(e))
            self.logger.exception(e)
            return HttpErrorResponse(ex.message)
        except Exception as e:
            ex = AgentException(AgentException.E_UNKNOWN, detail=e)
            self.logger.exception(e)
            return HttpErrorResponse(ex.message)
        else:
            try:
                self.logger.debug('Openning file %s' % class_file)
                fd = open(class_file, 'w')
                pickle.dump(p, fd)
                fd.close()
            except Exception as e:
                ex = AgentException(AgentException.E_CONFIG_COMMIT_FAILED, 
                    detail=e)
                self.logger.exception(ex.message)
                return HttpErrorResponse(ex.message)
            else:
                self.logger.debug('Created class file')
                return HttpJsonResponse()
	    
    def _stop(self, get_params, class_file, pClass):
        if not exists(class_file):
            return HttpErrorResponse(AgentException(
                AgentException.E_CONFIG_NOT_EXIST).message)
        try:
            try:
                fd = open(class_file, 'r')
                p = pickle.load(fd)
                fd.close()
            except Exception as e:
                ex = AgentException(AgentException.E_CONFIG_READ_FAILED, detail=e)
                self.logger.exception(ex.message)
                return HttpErrorResponse(ex.message)
            p.stop()
            remove(class_file)
            return HttpJsonResponse()
        except Exception as e:
            ex = AgentException(AgentException.E_UNKNOWN, detail=e)
            self.logger.exception(e)
            return HttpErrorResponse(ex.message)

    ################################################################################
    #                      methods executed on a MySQL Main                      #
    ################################################################################
    def _main_get_params(self, kwargs):
        ret = {}
        if 'main_server_id' not in kwargs:
            raise AgentException(AgentException.E_ARGS_MISSING, 'main_server_id')
        ret['main_server_id'] = kwargs.pop('main_server_id')
        if len(kwargs) != 0:
            raise AgentException(AgentException.E_ARGS_UNEXPECTED, kwargs.keys())
        ret['config'] = self.config_parser
        return ret
     
    def _subordinate_get_params(self, kwargs):
        ret = {}
        if 'subordinates' not in kwargs:
            raise AgentException(AgentException.E_ARGS_MISSING, 'subordinates')
        ret = kwargs.pop('subordinates')

        if len(kwargs) != 0:
            raise AgentException(AgentException.E_ARGS_UNEXPECTED, kwargs.keys())

        return ret
    
    def _set_password(self, username, password):
        if not exists(self.main_file):
            return HttpErrorResponse(AgentException(
                AgentException.E_CONFIG_NOT_EXIST).message)
        try:
            fd = open(self.main_file, 'r')
            p = pickle.load(fd)
            p.set_password(username, password)
            fd.close()
        except Exception as e:
            ex = AgentException(AgentException.E_CONFIG_READ_FAILED, 
			    role.MySQLMain.__name__, self.main_file, detail=e)
            self.logger.exception(ex.message)
            raise

    def _load_dump(self, f):
        if not exists(self.main_file):
            return HttpErrorResponse(AgentException(
                AgentException.E_CONFIG_NOT_EXIST).message)
        try:
            fd = open(self.main_file, 'r')
            p = pickle.load(fd)
            p.load_dump(f)
            fd.close()
        except Exception as e:
            ex = AgentException(AgentException.E_CONFIG_READ_FAILED, 
			    role.MySQLMain.__name__, self.main_file, detail=e)
            self.logger.exception(ex.message)
            raise
        
    @expose('POST')
    def create_main(self, kwargs):
        """Create a replication main"""
        self.logger.debug('Creating main')
        try: 
            kwargs = self._main_get_params(kwargs)
            self.logger.debug('main server id = %s' % kwargs['main_server_id']) 
        except AgentException as e:
            return HttpErrorResponse(e.message)
        else:
            with self.main_lock:
                return self._create(kwargs, self.main_file, role.MySQLMain)

    @expose('POST')
    def set_password(self, kwargs):
      """Create a replication main"""
      self.logger.debug('Updating password')
      try:
        if 'username' not in kwargs:
            raise AgentException(AgentException.E_ARGS_MISSING, 'username')
        username = kwargs.pop('username')
        if 'password' not in kwargs:
            raise AgentException(AgentException.E_ARGS_MISSING, 'password')
        password = kwargs.pop('password')
        if len(kwargs) != 0:
            raise AgentException(AgentException.E_ARGS_UNEXPECTED, kwargs.keys())
        self._set_password(username, password)
        return HttpJsonResponse()
      except AgentException as e:
        return HttpErrorResponse(e.message)
 
    @expose('UPLOAD')
    def load_dump(self, kwargs):
        self.logger.debug('Uploading mysql dump ') 
        self.logger.debug(kwargs) 
        #TODO: archive the dump?
        if 'mysqldump_file' not in kwargs:
             return HttpErrorResponse(AgentException(
                AgentException.E_ARGS_MISSING, 'mysqldump_file').message)
        file = kwargs.pop('mysqldump_file')
        if not isinstance(file, FileUploadField):
             return HttpErrorResponse(AgentException(
                AgentException.E_ARGS_INVALID, 
                    detail='"mysqldump_file" should be a file').message)
        try:
            self._load_dump(file.file)
        except AgentException as e:
            return HttpErrorResponse(e.message)
        else:
            return HttpJsonResponse()
        
    @expose('POST')
    def create_subordinate(self, kwargs):
        self.logger.debug('main in create_subordinate ')
        try: 
            ret = self._subordinate_get_params(kwargs)
            for server_id in ret:
                subordinate = ret[server_id]
                # TODO: Why do I receive the subordinate_ip in unicode??  
                from conpaas.services.galera.agent import client
                client.setup_subordinate(str(subordinate['ip']), subordinate['port'], self.my_ip)
                self.logger.debug('Created subordinate %s' % str(subordinate['ip']))
            return HttpJsonResponse()
        except AgentException as e:
            return HttpErrorResponse(e.message)

    @expose('UPLOAD')
    def setup_subordinate(self, kwargs):
        """Create a replication Subordinate"""
        self.logger.debug('subordinate in setup_subordinate ') 
        if 'main_host' not in kwargs:
            raise AgentException(AgentException.E_ARGS_MISSING, 'main_host')
        params = {"main_host" : kwargs["main_host"], "config":self.config_parser}
        self.logger.debug(params)
        with self.subordinate_lock:
            return self._create(params, self.subordinate_file, role.MySQLSubordinate)


