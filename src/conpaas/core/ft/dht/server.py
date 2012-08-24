'''
    Starts the DHT HTTP Server
'''

from BaseHTTPServer import HTTPServer
from SocketServer import ThreadingMixIn

from conpaas.core.http import AbstractRequestHandler
from conpaas.core import log
from conpaas.core.expose import exposed_functions

from . import node


class ServerRequestHandler(AbstractRequestHandler):

    def _do_dispatch(self, callback_type, callback_name, params):
      return self.server.callback_dict[callback_type][callback_name](self.server.peer, params)


class Server(ThreadingMixIn, HTTPServer):
    def __init__(self, my_address, my_id, remote_address=None): 

      HTTPServer.__init__(self, my_address, ServerRequestHandler)
      
      self.peer = node.Node(my_address, my_id, remote_address)

      # Register the callable functions
      self.callback_dict = {'GET': {}, 'POST': {}, 'UPLOAD': {}}
      for http_method in exposed_functions:
        for func_name in exposed_functions[http_method]:
          self._register_method(http_method, func_name,
                      exposed_functions[http_method][func_name])

    def _register_method(self, http_method, func_name, callback):
      self.callback_dict[http_method][func_name] = callback
