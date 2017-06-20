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
from threading import Lock


def do_zookeeper_read(zk, path):
	#print(path)
	data, stat = zk.get(path)
	#print('node info:', data)
	#print('node stat:', stat)

	children = zk.get_children(path)
	#print('node children:', children)

	return (data, stat, children)


class Manager:
	def __init__(self):
		self.zk_list = []
		self.lock = Lock()

	def append(self, zk):
		self.zk_list.append(zk)

	def sync(self):
		self.lock.acquire()
		print('# Sync start')

		for zk1 in self.zk_list:
			for zk2 in self.zk_list:
				if zk1 == zk2:
					continue

				zk1.read()
				zk2.read()

				# delete abnormal node
				for node in zk1.nonephemerals:
					if node not in zk2.ephemerals:
						print('# Delete: %s%s - %s is abnormal' % (zk1.name, zk1.path, node))
						zk1.delete(node)

				for node in zk2.nonephemerals:
					if node not in zk1.ephemerals:
						print('# Delete: %s%s - %s is abnormal' % (zk2.name, zk2.path, node))
						zk2.delete(node)


				# make node
				for node in zk1.ephemerals:
					if node in zk2.ephemerals:
						print('# Error: Duplicated ephemeral  %s%s - %s' % (zk1.name, zk1.path, node))
						continue
					
					if node not in zk2.nonephemerals:
						print('# Create: %s%s - %s' % (zk1.name, zk1.path, node))
						zk2.create(node, False)

				for node in zk2.ephemerals:
					if node in zk1.ephemerals:
						print('# Error: Duplicated ephemeral  %s%s - %s' % (zk2.name, zk2.path, node))
						continue
					
					if node not in zk1.nonephemerals:
						print('# Create: %s%s - %s' % (zk2.name, zk2.path, node))
						zk1.create(node, False)

				zk1.zk.get_children(zk1.path, watch=self.watch_children)
				zk2.zk.get_children(zk2.path, watch=self.watch_children)

		print('# Sync done')
		self.lock.release()
		

	def watch_children(self, event):
		print('# watch children called: ', event)
		self.sync()



class Zookeeper:
	def __init__(self, zk):
		zk, path = zk.split('/', 1)
		self.zk = KazooClient(zk)
		self.zk.start()
		self.name = zk
		self.path = '/' + path

		self.children = []
		self.ephemerals = []
		self.nonephemerals = []

		# safety check
		if '/arcus/cache_list/' not in self.path:
			print('# invalid zk node path (should include /arcus/cache_list)')
			sys.exit(0)

	def is_ephemeral(self, path):
		data, stat = self.zk.get(path)
		return stat.owner_session_id != None

	def read(self):
		self.children = []
		self.ephemerals = []
		self.nonephemerals = []

		data, stat = self.zk.get(self.path)
		self.children = self.zk.get_children(self.path)

		for child in self.children:
			if self.is_ephemeral(self.path + '/' + child):
				self.ephemerals.append(child)
			else:
				self.nonephemerals.append(child)
					
		print('# read zk(%s%s)' % (self.name, self.path), self.children)
		print('\tchildren', self.children)
		print('\tephemeral', self.ephemerals)
		print('\tnonephemeral', self.nonephemerals)

	def create(self, path, ephemeral=False):
		return self.zk.create(self.path + '/' + path, ephemeral = ephemeral)

	def delete(self, path):
		return self.zk.delete(self.path + '/' + path)


	


if __name__ == '__main__':
	if len(sys.argv) < 3:
		print("usage: python3 zk_sync.py [ZKADDR:PORT/PATH/CLOUD]+")
		sys.exit(0)

	mgr = Manager()
	for arg in sys.argv[1:]:
		zk = Zookeeper(arg)
		mgr.append(zk)
	
	mgr.sync()

	# for test
	'''
	for i in range(0, 10):
		print('######################################')
		zk.create('node%d' % i, True)
		time.sleep(1)
	'''

	input("Press any key to continue...")
	print('done')


