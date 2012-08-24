'''
# -- ConPaaS fault tolerance --
#
# Replication of the persistent state of the roles.
#
# @author Cristina Basescu (cristina.basescu@gmail.com)
'''

# Python 2.5 and later only
import time
import sys


from conpaas.core.abstract import client3

from conpaas.core.ft.dht import client3 as dht_client3

T = 10 # seconds

class ReplicateState(object):
    # state replication is done every T seconds
    def __init__(self, myip, myport, T):
        self.myip = myip
        self.myport = myport
        self.T = T

    def work(self):
        while True:
            try:
                time.sleep(self.T)
                roles = client3.get_roles(self.myip, int(self.myport))

                for j in range (0, len(roles)):
                    ID, name, role_port = roles[j].split(':')

                    state = client3.get_state(self.myip, role_port, name, ID)
                    key = dht_client3.get_key(ID)

                    # store state in temp file
                    path = '/tmp/'+str(key)
                    file = open(path, 'wb')
                    file.write(state)
                    file.close()

                    # replicate state in DHT
                    dht_client3.put((self.myip, 9990), key, path)
            except Exception as e:
                print(str(e))
