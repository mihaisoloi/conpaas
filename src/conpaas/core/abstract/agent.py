'''
    This class implements an abstract agent that must be
    extended by each service agent. 
    It provides function definitions for functions
    that must be implemented by each service agent: 
       - get_state
'''


import os.path, subprocess

from conpaas.core.expose import expose
from conpaas.core.controller import Controller
from conpaas.core.http import HttpJsonResponse, HttpErrorResponse,\
                         HttpFileDownloadResponse, HttpRequest,\
                         FileUploadField, HttpError, _http_post
from conpaas.core.log import create_logger

class AbstractAgent(object):

    def __init__(self,
                 config_parser, # config file
                 **kwargs):     # anything you can't send in config_parser
                                # (hopefully the new service won't need anything extra)
        self.VAR_TMP = config_parser.get('agent', 'VAR_TMP')
        self.VAR_CACHE = config_parser.get('agent', 'VAR_CACHE')
        self.VAR_RUN = config_parser.get('agent', 'VAR_RUN')
	self.CPS_HOME = config_parser.get('agent', 'CONPAAS_HOME')
        self.logger = create_logger(__name__)


    @expose('UPLOAD')
    def start_role(self, params):
        '''
            Starts the given role with the given state.

            @param params A dictionary containing the role to start
	                  and the state:
			  {'role':role, 'state': state}, where
			  state is a conpaas.core.http.FileUploadField
			  object containing the zip archive
 
        '''
        raise NotImplementedError	

    @expose('GET')
    def get_state(self, params):
        '''
	        Returns the persistant state of a role

	        @param params A dictionary containing the role for which to return
	                      the state: {'role':role}

	        @return Returns a conpaas.core.http.HttpFileDownloadResponse object
	                containing a zip archive of the persistent state of the role
	    '''
        raise NotImplementedError


