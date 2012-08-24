import httplib, json

from conpaas.core.http import HttpError, _jsonrpc_get, _jsonrpc_post, _http_post, _http_get

class ClientError(Exception): pass

def _check(response):
    code, body = response
    if code != httplib.OK:
        raise HttpError('Received http response code %d' % (code))
    try:
        data = json.loads(body)
    except Exception as e:
        raise ClientError(*e.args)
    if data['error']:
        raise ClientError(data['error'])
    else:
        return data['result']

def find_successor(address, id):
    method = 'find_successor'
    params = {'id': id}
    data = _check(_jsonrpc_get(address[0], address[1], '/', method, params=params))
    id = data['id']
    if id:
        address = (data['address'][0].encode('ascii', 'ignore'), data['address'][1])
    else:
        address = data['address']
    return id, address

def get_predecessor(address):
    method = 'get_predecessor'
    data = _check(_jsonrpc_get(address[0], address[1], '/', method))
    id = data['id']
    if id:
        address = (data['address'][0].encode('ascii', 'ignore'), data['address'][1])
    else:
        address = data['address']
    return id, address

def notify(address, my_address, my_id):
    method = 'notify'
    params = {}
    params['address'] = my_address
    params['id'] = my_id
    return _check(_jsonrpc_post(address[0], address[1], '/', method, params=params))

def ping_peer(address):
    method = 'ping_peer'
    return _check(_jsonrpc_get(address[0], address[1], '/', method))

def put(address, key, value):
    params = {'method': 'put',
              'key': key}
    files = {'value': value}
    return _check(_http_post(address[0], address[1], '/', params=params, files=files))

def get(address, key):
    method = 'get'
    params = {'key': key}
    code, body = _jsonrpc_get(address[0], address[1], '/', method, params=params)
    if code != httplib.OK:
        raise HttpError('Received http response code %d' % (code))
    return body

def update_keys(address, src_address, keys):
    method = 'update_keys'
    params = {}
    params['address'] = src_address
    params['keys'] = keys
    return _check(_jsonrpc_post(address[0], address[1], '/', method, params=params))
    
