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
from conpaas.services.mysql.agent import role 

from conpaas.core.https.server import HttpErrorResponse, HttpJsonResponse, FileUploadField
from conpaas.core.expose import expose

class MySQLAgent(BaseAgent):

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

    # TODO: clean code
    def _take_snapshot(self):
        if not exists(self.main_file):
            return HttpErrorResponse(AgentException(
                AgentException.E_CONFIG_NOT_EXIST).message)
        try:
            fd = open(self.main_file, 'r')
            p = pickle.load(fd)
            ret = p.take_snapshot()
            fd.close()
        except Exception as e:
            ex = AgentException(AgentException.E_CONFIG_READ_FAILED, 
			    role.MySQLMain.__name__, self.main_file, detail=e)
            self.logger.exception(ex.message)
            raise
        else:
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

    def _register_subordinate(self, subordinate_ip):
        if not exists(self.main_file):
            return HttpErrorResponse(AgentException(
                AgentException.E_CONFIG_NOT_EXIST).message)
        try:
            fd = open(self.main_file, 'r')
            p = pickle.load(fd)
            p.register_subordinate(subordinate_ip)
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
      '''
	 Creates a subordinate. Steps:
             1. do a mysqldump and record position
             2. send the dump to the subordinate agent and let it
	        start the mysql subordinate
      '''
      self.logger.debug('main in create_subordinate ')
      ret = self._take_snapshot()

      # TODO: why multiple keys?
      for position in ret.keys():
          main_log_file = ret[position]['binfile']
          main_log_pos = ret[position]['position']
          mysqldump_path = ret[position]['mysqldump_path']
      try: 
          kwargs = self._subordinate_get_params(kwargs)
	  for key in kwargs:
               # TODO: Why do I receive the subordinate_ip in unicode??  
               subordinate = kwargs[key]
               self._register_subordinate(str(subordinate['ip'])) 
               from conpaas.services.mysql.agent import client
               client.setup_subordinate(str(subordinate['ip']), subordinate['port'], \
                             key, \
			     self.my_ip, main_log_file, \
                             main_log_pos, mysqldump_path)
               self.logger.debug('Created subordinate %s' % str(subordinate['ip']))
          return HttpJsonResponse()
      except AgentException as e:
        return HttpErrorResponse(e.message)

    ################################################################################
    #                      methods executed on a MySQL Subordinate                       #
    ################################################################################
    def _subordinate_get_setup_params(self, kwargs):
        ret = {}
        if 'mysqldump_file' not in kwargs:
             return HttpErrorResponse(AgentException(
                AgentException.E_ARGS_MISSING, 'mysqldump_file').message)
        file = kwargs.pop('mysqldump_file')
        if not isinstance(file, FileUploadField):
             return HttpErrorResponse(AgentException(
                AgentException.E_ARGS_INVALID, 
                    detail='"mysqldump_file" should be a file').message)
        ret['mysqldump_file'] = file.file

        if 'main_host' not in kwargs:
            return HttpErrorResponse(AgentException(
                AgentException.E_ARGS_MISSING, 'main_host').message)
        ret['main_host'] = kwargs.pop('main_host')
  
        if 'main_log_file' not in kwargs:
            return HttpErrorResponse(AgentException(
                AgentException.E_ARGS_MISSING, 'main_log_file').message)
        ret['main_log_file'] = kwargs.pop('main_log_file')

        if 'main_log_pos' not in kwargs:
            return HttpErrorResponse(AgentException(
                AgentException.E_ARGS_MISSING, 'main_log_pos').message)
        ret['main_log_pos'] = kwargs.pop('main_log_pos')

        if 'subordinate_server_id' not in kwargs:
            return HttpErrorResponse(AgentException(
                AgentException.E_ARGS_MISSING, 'subordinate_server_id').message)
        ret['subordinate_server_id'] = kwargs.pop('subordinate_server_id')

        if len(kwargs) != 0:
            return HttpErrorResponse(AgentException(
                AgentException.E_ARGS_UNEXPECTED, kwargs.keys()).message)
      
        ret['config'] = self.config_parser
        return ret

    @expose('UPLOAD')
    def setup_subordinate(self, kwargs):
        self.logger.debug('subordinate in setup_subordinate ') 
        self.logger.debug(kwargs)
        #TODO: archive the dump?
        """Create a replication Subordinate"""
        try:
            kwargs = self._subordinate_get_setup_params(kwargs)
        except AgentException as e:
            return HttpErrorResponse(e.message)
        else:
            with self.subordinate_lock:
                return self._create(kwargs, self.subordinate_file, role.MySQLSubordinate)

    # TODO: Update subordinate - if manager changes! 
