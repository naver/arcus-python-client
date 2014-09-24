#
# arcus-python-client - Arcus python client drvier
# Copyright 2014 NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
 

import getpass
import sys
import telnetlib

from kazoo.client import KazooClient
import kazoo
from optparse import OptionParser


lists = []

def get_arcus_node_list(addr, code):
	zk = KazooClient(addr)
	zk.start()
	children = zk.get_children('/arcus/cache_list/' + code)

	ret = []
	for child in children:
		addr, name = child.split('-')
		ip, port = addr.split(':')
		ret.append((name, port, ip))

	return ret



if __name__ == '__main__':
	usage = "usage: %prog [options]"
	parser = OptionParser(usage=usage, version="%prog 1.0")
	parser.add_option('-a', '--address', dest='address', default='', help='zookeeper address')
	parser.add_option('-s', '--service', dest='service', default='', help='service code')
	parser.add_option('-c', '--command', dest='command', default='', help='command')

	(options, args) = parser.parse_args()

	lists = get_arcus_node_list(options.address, options.service)

	for node in lists:
		tn = telnetlib.Telnet(node[0], node[1])
		tn.write(bytes(options.command + '\n', 'utf-8'))
		print ('[%s:%s] %s - %s' % (node[0], node[1], options.command, tn.read_until(bytes('END', 'utf-8')).decode('utf-8')))
		tn.write(bytes('quit\n', 'utf-8'))

	



