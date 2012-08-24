'''
# -- ConPaaS fault tolerance --
#
# Client for the fault tolerance process.
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

from conpaas.core.http3 import _jsonrpc_get, _jsonrpc_post, _http_post
import http.client, json
import sys

def _check(response):
    code, body = response
    if code != http.client.OK: raise AgentException('Received http response code %d' % (code))
    try: data = json.loads(body)
    except Exception as e: raise AgentException(*e.args)
    if data['error']: raise AgentException(data['error'])
    else: return True


# Used by the membership component to exchange views
def exchange_views(host, port, buffer):
    method = 'server'
    buffer['component'] = 'membership'
    params = buffer
    resp = _jsonrpc_post(host, port, '/', method, params=params)
    check = _check(resp)
    if(check):
        return resp
    return False

# Used by the recovery component to recover roles
def recover_role(host, port, op, buffer):
    if(op == 'new'):
        method = 'recover_on_new_VM'
    elif(op == 'existent'):
        method = 'recover_on_existent_VM'
    else:
        method = 'recover_mixed'

    buffer['component'] = 'recovery'
    params = buffer

    print("trimit recover ")
    print("host="+host+"port="+str(port)+"params="+str(params))
    sys.stdout.flush()

    code, body = _jsonrpc_post(host, port, '/', method, params=params)
    if code != http.client.OK:
        raise HttpError('Received http response code %d' % (code))
    data = json.loads(body)
    print("back in recovered_role "+str(data))
    return data['result']['recovered']
