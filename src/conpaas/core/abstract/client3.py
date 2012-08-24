'''
    These methods are called by the FT component to discuss with the manager or the agent.
    Because the FT component is written in Python v.3, this code is also for Python v.3

'''

#import httplib, json
## Python 3
import http.client, json
## end

from conpaas.core.http3 import HttpError, _jsonrpc_get, _jsonrpc_post, _http_post, _http_get, \
                               _ft_jsonrpc_get                                

class ClientError(Exception): pass

def _check(response):
    code, body = response
    if code != http.client.OK:
        raise HttpError('Received http response code %d' % (code))
    try:
        data = json.loads(body)
    except Exception as e:
        raise ClientError(*e.args)
    if data['error']:
        raise ClientError(data['error'])
    else:
        return data['result']

##### FT
def get_state(host, port, role_name, role_id):
    params = {'role_name': role_name,
              'role_id': role_id}
    method = 'get_state'
    code, body = _ft_jsonrpc_get(host, port, '/', method, params=params)
    if code != http.client.OK:
        raise HttpError('Received http response code %d' % (code))
    return body

def start_role(host, port, role_name, role_id, state_file=None, new=None):
    params = {
            'method': 'start_role',
            'role_name': role_name,
            'role_id': role_id
            }
    if new != None:
        params['new'] = True
    files = []
    if state_file != None:
        files = {'state': state_file}
    #TODO: Check if successful
    return _check(_http_post(host, port, '/', params, files=files))

def is_alive(host, port, role_name, role_id):
    params = {'role_name': role_name,
              'role_id': role_id}
    method = 'is_alive'
    code, body = _ft_jsonrpc_get(host, port, '/', method, params=params)
    if code != http.client.OK:
        raise HttpError('Received http response code %d' % (code))
    #TODO:parse body; return True or False
    data = json.loads(body.decode("utf-8"))
    return data['result']['alive']

def get_roles(host, port):
    method = 'get_roles'
    code, body = _ft_jsonrpc_get(host, port, '/', method)
    if code != http.client.OK:
        raise HttpError('Received http response code %d' % (code))
    data = json.loads(body.decode("utf-8"))
    return data['result']

def get_role_id(host, port):
    method = 'get_role_id'
    code, body = _ft_jsonrpc_get(host, port, '/', method)
    if code != http.client.OK:
        raise HttpError('Received http response code %d' % (code))
    data = json.loads(body.decode("utf-8"))
    return data['result']['id']

def notify_manager(host, port, role_ip, role_port, role_id, role_name, op):
    method = 'notify_manager'
    params = {'role_ip': role_ip,
              'role_port': role_port,
              'role_id': role_id,
              'role_name': role_name,
              'op': op
             }
    return _check(_jsonrpc_post(host, port, '/', method, params=params))
##### FT
