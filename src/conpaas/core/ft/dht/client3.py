#import httplib, json
## Python 3
import http.client, json, binascii
## end

from conpaas.core.http3 import HttpError, _jsonrpc_get, _jsonrpc_post, _http_post, _http_get, \
                               _ft_jsonrpc_get                                
class ClientError(Exception): pass

def get_key(id):
    return binascii.crc32(bytes(id, 'utf-8'))

def _check(response):
    code, body = response
    if code != http.client.OK:
        raise HttpError('Received http response code %d' % (code))
    try:
        data = json.loads(body.decode('utf-8'))
    except Exception as e:
        raise ClientError(*e.args)
    if data['error']:
        raise ClientError(data['error'])
    else:
        return data['result']

def find_successor(address, id):
    method = 'find_successor'
    params = {'id': id}
    data = _check(_ft_jsonrpc_get(address[0], address[1], '/', method, params=params))
    id = data['id']
    if id:
        address = (data['address'][0].encode('ascii', 'ignore'), data['address'][1])
    else:
        address = data['address']
    return id, address

def put(address, key, value):
    params = {'method': 'put',
              'key': key}
    files = {'value': value}
    code, body = _http_post(address[0], address[1], '/', params=params, files=files)
    if code != http.client.OK:
        raise HttpError('Received http response code %d' % (code))
    return body

def get(address, key):
    method = 'get'
    params = {'key': key}
    code, body = _ft_jsonrpc_get(address[0], address[1], '/', method, params=params)
    if code != http.client.OK:
        raise HttpError('Received http response code %d' % (code))
    return body

def get_successors(address):
    method = 'get_successors'
    data = _check(_ft_jsonrpc_get(address[0], address[1], '/', method))
    return data['successors']

