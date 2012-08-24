'''
    This class implements an abstract manager that must be
    extended by each service manager. It provides
    implementations for the functions that are not service
    specific:
       - start_ft

    Also, it provides function definitions for functions
    that must be implemented by each service manager to serve
    to the FT component:
       - get_state
       - start_role
       - etc. (see file core/abstract/client3.py for more details)

    It could also provide function definitions for functions common
    to all services:
       - add_nodes
       - remove_nodes
       - get_log
       - etc.
'''

import os.path, subprocess

from conpaas.core.expose import expose
from conpaas.core.controller import Controller
from conpaas.core.http import HttpJsonResponse, HttpErrorResponse,\
                         HttpFileDownloadResponse, HttpRequest,\
                         FileUploadField, HttpError, _http_post
from conpaas.core.log import create_logger

class AbstractManager(object):

    # Manager states - Used by the frontend
    S_INIT = 'INIT'         # manager initialized but not yet started
    S_PROLOGUE = 'PROLOGUE' # manager is starting up
    S_RUNNING = 'RUNNING'   # manager is running
    S_ADAPTING = 'ADAPTING' # manager is in a transient state - frontend will keep
                            # polling until manager out of transient state 
    S_EPILOGUE = 'EPILOGUE' # manager is shutting down
    S_STOPPED = 'STOPPED'   # manager stopped
    S_ERROR = 'ERROR'       # manager is in error state

    def __init__(self,
                 config_parser, # config file
                 **kwargs):     # anything you can't send in config_parser
                                # (hopefully the new service won't need anything extra)
        self.config_parser = config_parser
        self.logger = create_logger(__name__)  

        # For the FT component: if we want to start a role on the manager
        # we must first start an agent and then start a role in the agent
        # self.agent_started = False

    @expose('POST')
    def start_ft(self, params):
        path = os.path.join(self.config_parser.get('manager', 'CONPAAS_HOME'),
                            "scripts", "ft", "start_manager.py")
        subprocess.Popen([path, '-c', '/root/config.cfg'], close_fds=True)
        return HttpJsonResponse({'state': 'excelenta'})

    @expose('POST')
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
	        Returns the persistant state of the manager

	        @param params A dictionary containing the role for which to return
	                      the state: {'role':role}. role must be 'manager'

	        @return Returns a conpaas.core.http.HttpFileDownloadResponse object
	                containing a zip archive of the persistent state of the role
	    '''
        raise NotImplementedError
    
    @expose('GET')
    def is_alive(self, params):
        '''
	        Returns True if manager is alive
	    '''
        raise NotImplementedError


