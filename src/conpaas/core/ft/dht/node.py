'''
    Implementation of a self-stabilizing Chord DHT.
    It is used by the FT component to store/replicate the
    state of the roles.

    To communicate with the DHT, the FT component uses the
    HTTP client methods implemented in the core/ft/dht/client3.py file.
'''

import threading
import os

from conpaas.core.expose import expose
from conpaas.core.controller import Controller
from conpaas.core.http import HttpJsonResponse, HttpErrorResponse,\
                         HttpFileDownloadResponse, HttpRequest,\
                         FileUploadField, HttpError, _http_post
from . import client
from . import rwlock

import sys

# network size (< 2^SIZE)
SIZE = 32 
MAX = 2**SIZE
N = 3 

keystore_path = '/root/keystore'

def comparator(left_op, right_op):
    def cmp(n, lo, hi):
        if lo < hi:
            return left_op(lo, n) and right_op(n, hi)
        return left_op(lo, n) or right_op(n, hi)
    return cmp

from operator import le, lt

left_range = comparator(le, lt) # Returns True if i in [j, k)
strict_range = comparator(lt, lt) # Returns True if i in (j, k)
right_range = comparator(lt, le) # Returns True if i in (j, k]


class Node(object):
    def __init__(self, my_address, my_id, remote_address=None): 
        self.my_id = my_id % MAX	
        self.my_address = my_address
       
        # Protects against concurrent access to fingers, successor, predecessor,
        # and routing table (for simplicity, we use just one lock for all)
        self._lock = rwlock.ReadWriteLock()

        # Initialize routing_table
        self.routing_table = {my_id : my_address}
        
        # Initialize fingers:
        self.fingers = {}
        for i in range(SIZE):
            self.fingers[i] = None
        
        if remote_address:
            self._join(remote_address)
        else:
            self._create()

        # The next finger to be updated by the periodic function fix_fingers
        self.next_finger = 0
        
	# List with the keys that are stored on this node and the associated lock
        self.stored_keys = []
        self._keys_lock = rwlock.ReadWriteLock()

        # Start the periodic functions:
        TaskThread(self._stabilize, 0.5).start()
        TaskThread(self._update_keys, 0.5).start()
        #TaskThread(self._print_tables, 5).start()


    def _update_all(self, id):
        '''
            Called after we detected that a successor failed

            @param id id of the successor
        '''
        with self._lock.writelock:
            if id == self.successor_list[0]:
                # _update_all could be called twice for the same successor that failed
                # (it's called in _find_successor and in stabilize, two separate threads)
                # so we have this "if" to check if the successor we try to remove
                # has already been removed
                self.successor_list.pop(0)
                self.fingers[0] = self.successor_list[0]
                for i in range(SIZE):
                    if self.fingers[i] and self.fingers[i] == id:
                        self.fingers[i] = None
                if id in self.routing_table:
                    del self.routing_table[id]

    def _create_successor_list(self):
        '''
            We create a temporary successor_list and routing table for it
            and update the existing ones only at the end.
        '''
        tmp_successor_list = []
        tmp_routing_table = {}
        next_id = self.my_id + 1
        for i in range(N):
            id, address = client.find_successor(self.my_address, next_id)
            #print i, id, next_id, tmp_successor_list
            if len(tmp_successor_list) > 0 \
                   and (id == self.my_id \
                   or id == tmp_successor_list[0]) :
                break
            tmp_successor_list.append(id)
            tmp_routing_table[id] = address
            next_id = id + 1
        with self._lock.writelock:
            self.successor_list = tmp_successor_list
            for key in tmp_routing_table:
                self.routing_table[key] = tmp_routing_table[key]

    def _join(self, address):
        ''' 
            Join a Chord ring containing node with given address.

            @param address the bootstrapping node, tuple (ip_address, port)
        '''
        self.predecessor = None
        id, address = client.find_successor(address, self.my_id)
        self.fingers[0] = id
        self.routing_table[id] = address
        self.successor_list = [id]
   
    def _create(self):
        '''
            Create a Chord ring.
        '''
        self.predecessor = None 
        self.successor_list = [self.my_id]

    def _send_find_successor(self, n0, i0, id):
        with self._lock.readlock:
            successor = self.successor_list[0]
            successor_address = self.routing_table[successor]
        if n0 == successor:
            ''' We try to send to our successor, and if that fails, we send to the next successor'''
            # TODO: Protect len(self.succe..)
            while True and len(self.successor_list):
                try:
                    id, address = client.find_successor(successor_address, id)
                    return id, address
                except Exception as e:
                    # successor failed, update all
                    print 'Successor %s failed' % self.successor_list[0]
                    self._update_all(successor)
                with self._lock.readlock:
                    successor = self.successor_list[0]
                    successor_address = self.routing_table[successor]
        else:
            ''' We try to send to the finger, if that fails we make the finger None and retry the request. '''
            try:
                id, address = client.find_successor(self.routing_table[n0], id)
                return id, address
            except Exception as e:
                #print 'Finger failed %s ' % n0
                self.fingers[i0] = None
                #print self.fingers
                n0, i0 = self._closest_preceding_node(id)
                return self._send_find_successor(n0, i0, id)

    @expose('GET')
    def find_successor(self, params):
            '''
            Find the successor of id
            
            @param params['id']
            @return ip_address, port, id of the requested node
            '''
            id = int(params['id'])
            #print 'FS Searching for: %s' % id
            #print 'FS successor: %s' % self.successor_list[0]
            #print 'FS predecessor: %s' % self.predecessor
            #print 'FS successor_list: %s' % self.successor_list
            #print 'FS fingers: %s' % self.fingers
            with self._lock.readlock:
                successor = self.successor_list[0]
                successor_address = self.routing_table[successor]
            if right_range(id, self.my_id, successor):
                return HttpJsonResponse({'id': successor,
                                         'address': successor_address})
            else:
                # forward the query around the circle
                #print 'FS pe closest '
                n0, i0 = self._closest_preceding_node(id)
                #print 'FS closest: %s' % n0
                id, address = self._send_find_successor(n0, i0, id);
                #print 'FS response: %s' % id
                return HttpJsonResponse({'id': id,
                                         'address': address})

    def _closest_preceding_node(self, id):
        '''
	        Search the local table for the highest predecessor of id
	    '''
        with self._lock.readlock:
            for i in range(SIZE):
                finger = self.fingers[SIZE - i - 1]
                if finger:
                    if strict_range(finger, self.my_id, id):
                        return finger, SIZE - i - 1
            return self.my_id, None # This shouldn't happen

    ''' 
        Succeeds if not all N successors fail until
        the next stabilization of the successors list
    '''
    def _send_notify(self):
        while True and len(self.successor_list):
            with self._lock.readlock:
                successor = self.successor_list[0]
                successor_address = self.routing_table[successor]
            try:
                client.notify(successor_address, self.my_address, self.my_id)
                break
            except Exception as e:
                # successor failed, update list and self.successor
                self._update_all(successor)

    ''' 
        Succeeds if not all N successors fail until
        the next stabilization of the successors list
    '''
    def _get_predecessor(self):
        while True and len(self.successor_list):
            with self._lock.readlock:
                successor = self.successor_list[0]
                successor_address = self.routing_table[successor]
            try:
                id, address = client.get_predecessor(successor_address)
                return id, address
            except Exception as e:
                # successor failed, update list and self.successor
                print '**** Successor %s failed' % self.successor_list[0]
                self._update_all(successor)
        return None, None

    def _stabilize(self):
        '''
            Called periodically. We ask our successor 
            about its predecessor, we verify if our immediate
            successor is consistent, and tell our successor about it.
        '''
        self._create_successor_list()
        id, address = self._get_predecessor()
        if id:
            with self._lock.writelock:
                if strict_range(id, self.my_id, self.successor_list[0]):
                    self.successor_list[0] = id
                    self.routing_table[id] = address
                    self.fingers[0] = id
        self._send_notify()
        self._fix_fingers()

    @expose('GET')
    def get_predecessor(self, params):
        if self.predecessor != None and self._check_predecessor() == False:
            self.predecessor = None
        with self._lock.readlock:
            if self.predecessor is None:
                address = None 
            else:
                address = self.routing_table[self.predecessor]
            return HttpJsonResponse({'id': self.predecessor,
                                     'address': address})

    @expose('POST')
    def notify(self, params):
        '''
            A node thinks it might be our predecessor.
     
            @param params['address']
            @param params['id']
        '''
        if self.predecessor != None and self._check_predecessor() == False:
            with self._lock.writelock:
                self.predecessor = None
        with self._lock.writelock:
            address = (params['address'][0].encode('ascii', 'ignore'), params['address'][1])
            id = params['id']
            if self.predecessor is None \
                    or strict_range(id, self.predecessor, self.my_id): 
                self.predecessor = id
                self.routing_table[id] = address
            return HttpJsonResponse({})

    def _fix_fingers(self):
        '''
        '''
        if self.next_finger == SIZE:
             self.next_finger = 0
        #print 'Fix finger %s val: %s' % (self.next_finger, self.my_id + 2**self.next_finger)
        id, address = client.find_successor(self.my_address, self.my_id + 2**self.next_finger)
        #print 'FF: id_succ: %s %s' % (id, address)
        with self._lock.writelock:
            self.fingers[self.next_finger] = id
            self.routing_table[id] = address
        self.next_finger += 1

    @expose('POST')
    def update_keys(self, params):
        keys = params['keys']
        address = (params['address'][0].encode('ascii', 'ignore'), params['address'][1])
	#print 'Received keys: %s' % keys
        for key in keys:
            try:
                key = key.encode('ascii', 'ignore') 
                file_bytes = client.get(address, int(key))
                fd = open(os.path.join(keystore_path, key), 'wb')
		fd.write(file_bytes)
                with self._keys_lock.writelock:
		    if not key in self.stored_keys:
                        self.stored_keys.append(key)
            except:
                pass
        return HttpJsonResponse({})

    def _update_keys(self):
        '''
            Called periodicaly.
            Send to all those in my successor list a request to copy my keys.
	    TODO: Make this more efficiently (git repo/ send only when key updated, etc.)
        '''
        with self._lock.readlock:
            successors = self.successor_list
            routing_table = self.routing_table
            predecessor = self.predecessor
	with self._keys_lock.readlock:
            my_keys = []
            for key in self.stored_keys:
                if right_range(int(key), predecessor, self.my_id):
                    my_keys.append(key)
        #print 'Sending my_keys:%s to my successors' % str(my_keys)
        if len(my_keys) > 0:
            for successor in successors:
                #print 'Sending my_keys:%s to %s' % (str(my_keys), successor)
                try:
                    client.update_keys(routing_table[successor], self.my_address, my_keys)
                except:
                    pass
 
    def _check_predecessor(self):
        with self._lock.readlock:
            predecessor = self.routing_table[self.predecessor]
        try:
            client.ping_peer(predecessor)
            return True
        except Exception as e:
            print 'Predecessor %s failed' % self.predecessor
            return False

    @expose('GET')
    def ping_peer(self, params):
        return HttpJsonResponse({})

    @expose('UPLOAD')
    def put(self, params):
        key = params['key']
        value = params['value'].file.read()
        while True:
            try:
                id, address = client.find_successor(self.my_address, int(key))
                if id == self.my_id:
                    with self._keys_lock.writelock:
		        if not key in self.stored_keys:
                            self.stored_keys.append(key)
                    fd = open(os.path.join(keystore_path, key), 'wb')
                    fd.write(value)
                    fd.close()
                else:
                    tmp_path = os.path.join('/tmp', key)
                    fd = open(tmp_path, 'wb')
                    fd.write(value)
                    fd.close()
                    client.put(address, key, tmp_path)
                    os.remove(tmp_path)
                print 'Key %s successfully stored into the system' % key
                return HttpJsonResponse({})
            except Exception as e:
                print e
                pass

    @expose('GET')
    def get(self, params):
        key = params['key']
        while True:
            try:
                id, address = client.find_successor(self.my_address, int(key))
                if id == self.my_id:
                    filepath = os.path.join(keystore_path, str(key))
                else:
                    file_bytes = client.get(address, key)
                    filepath = os.path.join('/tmp', str(key))
                    fd = open(filepath, 'wb')
                    fd.write(file_bytes)
                return HttpFileDownloadResponse('state.zip', filepath)
            except:
                pass
    
    @expose('GET')
    def get_successors(self, params):
        successors = []
        with self._lock.readlock:
            for successor in self.successor_list:
                address = self.routing_table[successor]
		successors.append(address[0] + ':' + str(address[1]))
        return HttpJsonResponse({'successors': successors})

    def _print_tables(self):
        with self._lock.readlock:
            print 'successor: %s' % self.successor_list[0]
            print 'predecessor: %s' % self.predecessor
            print 'Fingers:\n %s' % self.fingers
            print 'Routing table:\n %s' % self.routing_table
            print 'Successor list:'
            print self.successor_list
            print 'Stored keys:'
            with self._keys_lock.readlock:
                print self.stored_keys

class TaskThread(threading.Thread):
    """Thread that executes a task every N seconds"""
            
    def __init__(self, function, interval=5):
        threading.Thread.__init__(self)
        self._finished = threading.Event()
        self._interval = interval
        self.function = function
                                                    
    def setInterval(self, iddnterval):
        """Set the number of seconds we sleep between executing our task"""
        self._interval = interval
                                                                    
    def shutdown(self):
        """Stop this thread"""
        self._finished.set()
                                                                                            
    def run(self):
        while 1:
            if self._finished.isSet():
                return
            self.function()
            # sleep for interval or until shutdown
            self._finished.wait(self._interval)
