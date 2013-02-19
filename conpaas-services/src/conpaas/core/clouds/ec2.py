'''
Copyright (c) 2010-2012, Contrail consortium.
All rights reserved.

Redistribution and use in source and binary forms,
with or without modification, are permitted provided
that the following conditions are met:

 1. Redistributions of source code must retain the
    above copyright notice, this list of conditions
    and the following disclaimer.
 2. Redistributions in binary form must reproduce
    the above copyright notice, this list of
    conditions and the following disclaimer in the
    documentation and/or other materials provided
    with the distribution.
 3. Neither the name of the Contrail consortium nor the
    names of its contributors may be used to endorse
    or promote products derived from this software
    without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
'''

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.compute.base import NodeImage

from .base import Cloud


class EC2Cloud(Cloud):

    def __init__(self, cloud_name, iaas_config):
        Cloud.__init__(self, cloud_name)

        cloud_params = ['USER', 'PASSWORD',
                        'IMAGE_ID', 'SIZE_ID',
                        'SECURITY_GROUP_NAME',
                        'KEY_NAME', 'REGION']

        self._check_cloud_params(iaas_config, cloud_params)

        # TODO: multiple img_id, key_name?
        self.user = iaas_config.get(cloud_name, 'USER')
        self.passwd = iaas_config.get(cloud_name, 'PASSWORD')
        self.img_id = iaas_config.get(cloud_name, 'IMAGE_ID')
        self.size_id = iaas_config.get(cloud_name, 'SIZE_ID')
        self.key_name = iaas_config.get(cloud_name, 'KEY_NAME')
        self.sg = iaas_config.get(cloud_name, 'SECURITY_GROUP_NAME')
        self.ec2_region = iaas_config.get(cloud_name, 'REGION')

    def get_cloud_type(self):
        return 'ec2'

    # connect to ec2 cloud
    def _connect(self):
        if self.ec2_region == "ec2.us-east-1.amazonaws.com":
            EC2Driver = get_driver(Provider.EC2_US_EAST)
        elif self.ec2_region == "ec2.us-west-2.amazonaws.com":
            EC2Driver = get_driver(Provider.EC2_US_WEST_OREGON)
        elif self.ec2_region == "ec2.eu-west-1.amazonaws.com":
            EC2Driver = get_driver(Provider.EC2_EU_WEST)

        self.driver = EC2Driver(self.user,
                                self.passwd)
        self.connected = True

    def config(self, config_params={}, context=None):
        if 'inst_type' in config_params:
            self.size_id = config_params['inst_type']

        if context is not None:
            self.cx = context

    def new_instances(self, count, name='conpaas'):
        if self.connected is False:
            self._connect()

        size = [i for i in self.driver.list_sizes()
                if i.id == self.size_id][0]
        img = NodeImage(self.img_id, '', None)
        kwargs = {'size': size,
                  'image': img,
                  'name': name}
        kwargs['ex_mincount'] = str(count)
        kwargs['ex_maxcount'] = str(count)
        kwargs['ex_securitygroup'] = self.sg
        kwargs['ex_keyname'] = self.key_name
        kwargs['ex_userdata'] = self.cx
        return self._create_service_nodes(self.driver.create_node(**kwargs))
