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
 
from kazoo.client import KazooClient
import kazoo
import sys, os, time
import re
from optparse import OptionParser


def do_zookeeper_read(addr, path):
	print(path)
	zk = KazooClient(addr)
	zk.start()
	data, stat = zk.get(path)
	print('node info:', data)
	print('node stat:', stat)

	children = zk.get_children(path)
	print('node children:', children)

def do_zookeeper_create(addr, path, value):
	print(path)
	zk = KazooClient(addr)
	zk.start()
	zk.create(path, bytes(value, 'utf-8'))

	do_zookeeper_read(addr, path)

def do_zookeeper_delete(addr, path):
	print(path)
	zk = KazooClient(addr)
	zk.start()
	zk.delete(path)

	try:
		do_zookeeper_read(addr, path)
	except kazoo.exceptions.NoNodeError:
		print('deleted')
	
def do_zookeeper_update(addr, path, value):
	print(path)
	zk = KazooClient(addr)
	zk.start()
	zk.set(path, bytes(value, 'utf-8'))

	do_zookeeper_read(addr, path)


if __name__ == '__main__':


	usage = "usage: %prog [options]"
	parser = OptionParser(usage=usage, version="%prog 1.0")
	parser.add_option('-a', '--address', dest='address', default='', help='zookeeper address')
	parser.add_option('-n', '--node', dest='node', default='', help='zookeeper node path')
	parser.add_option('-r', '--read', dest='read', default=False, help='zookeeper node read', action='store_true')
	parser.add_option('-c', '--create', dest='create', default='', help='zookeeper node create')
	parser.add_option('-d', '--delete', dest='delete', default=False, help='zookeeper node delete', action='store_true')
	parser.add_option('-u', '--update', dest='update', default='', help='zookeeper node update')

	(options, args) = parser.parse_args()

	if options.read:
		do_zookeeper_read(options.address, options.node)
	elif options.create != '':
		do_zookeeper_create(options.address, options.node, options.create)
	elif options.delete:
		do_zookeeper_delete(options.address, options.node)
	elif options.update != '':
		do_zookeeper_update(options.address, options.node, options.update)


	parser.print_usage();

