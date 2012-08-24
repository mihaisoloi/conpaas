'''
# -- ConPaaS fault tolerance --
#
# Fault tolerance server, used by the membership and recovery components
#
# @author Cristina Basescu (cristina.basescu@gmail.com) and ielhelw
'''

from http.server import HTTPServer
from socketserver import ThreadingMixIn

from conpaas.core.http3 import AbstractRequestHandler
from conpaas.core import log
from conpaas.core.expose import exposed_functions
from conpaas.core.mservices import services

from conpaas.core.ft.membership import GossipPeerSampling

import sys

class ManagerRequestHandler(AbstractRequestHandler):
    # the 'component' key specifies for which component the request is
    def _do_dispatch(self, callback_type, callback_name, params):
        if(params['component'] == 'membership'):
            self.server.instance = self.server.membership
        if(params['component'] == 'recovery'):
            self.server.instance = self.server.recovery
        return self.server.callback_dict[callback_type][callback_name](self.server.instance, params)

class FTServer(ThreadingMixIn, HTTPServer):

    """
    This class creates the requested manager. Each Service Manager class
    must fill in a dictionary named exposed_functions that contains the functions
    visible from outside, i.e. callable by the frontend. This is done by using
    the expose package.
    """

    def __init__(self,
                 server_address,
                membership,
                recovery,
                 **kwargs):

        HTTPServer.__init__(self, server_address, ManagerRequestHandler)

        self.whitelist_addresses = []

        # Instantiate the membership and recovery
        self.membership = membership
        self.recovery = recovery

        # Register the callable functions
        self.callback_dict = {'GET': {}, 'POST': {}, 'UPLOAD': {}}
        for http_method in exposed_functions:
            for func_name in exposed_functions[http_method]:
                self._register_method(http_method, func_name,
                            exposed_functions[http_method][func_name])

    def _register_method(self, http_method, func_name, callback):
        self.callback_dict[http_method][func_name] = callback
