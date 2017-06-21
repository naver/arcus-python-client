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
from datetime import datetime

def log(*arg):
	str = '[%s] ' % datetime.now()

	for a in arg:
		str += a.__str__()

	print(str)

class Manager:
	def __init__(self):
		self.zk_list = []
		self.lock = Lock()

	def append(self, zk):
		self.zk_list.append(zk)

	def sync(self):
		self.lock.acquire()
		log('# Sync start')

		# read children
		for zk in self.zk_list:
			zk.read()

		# make new ehphemeral node
		for zk1 in self.zk_list:
			for zk2 in self.zk_list:
				if zk1 == zk2:
					continue

				# make node
				for node in zk1.ephemerals:
					if node in zk2.ephemerals:
						log('# Error: Duplicated ephemeral  %s%s - %s' % (zk1.name, zk1.path, node))
						continue
					
					if node not in zk2.nonephemerals:
						log('# Create: %s%s - %s' % (zk1.name, zk1.path, node))
						zk2.create(node, False)


		# delete old nonehphemeral node
		for zk1 in self.zk_list:
			for node in zk1.nonephemerals:
				flag = False
				for zk2 in self.zk_list:
					if zk1 == zk2:
						continue

					if node in zk2.ephemerals:
						flag = True
						break

				if flag == False:
					# delete abnormal node
					log('# Delete: %s%s - %s is abnormal' % (zk1.name, zk1.path, node))
					zk1.delete(node)

		# watch children again
		for zk in self.zk_list:
			zk.zk.get_children(zk.path, watch=self.watch_children)

		log('# Sync result')
		for zk in self.zk_list:
			zk.read()
		log('# Sync done')
		self.lock.release()
		
	def watch_children(self, event):
		log('# watch children called: ', event)
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
			log('# invalid zk node path (should include /arcus/cache_list)')
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
					
		log('# read zk(%s%s)' % (self.name, self.path))
		log('\tchildren: ', self.children)
		log('\tephemeral: ', self.ephemerals)
		log('\tnonephemeral: ', self.nonephemerals)

	def create(self, path, ephemeral=False):
		return self.zk.create(self.path + '/' + path, ephemeral = ephemeral)

	def delete(self, path):
		return self.zk.delete(self.path + '/' + path)


	


if __name__ == '__main__':
	# for test
	if len(sys.argv) == 1:
		# add here for test like below
		# sys.argv.append('zk1.addr.com:17288/arcus/cache_list/cloud_1')
		# sys.argv.append('zk2.addr.com:17288/arcus/cache_list/cloud_2')
		pass

	if len(sys.argv) < 3:
		log("usage: python3 zk_sync.py [ZKADDR:PORT/PATH/CLOUD]+")
		sys.exit(0)

	mgr = Manager()
	for arg in sys.argv[1:]:
		zk = Zookeeper(arg)
		mgr.append(zk)
	
	mgr.sync()

	# for test
	'''
	for i in range(0, 10):
		log('######################################')
		zk.create('node%d' % i, True)
		time.sleep(1)

	for i in range(0, 10):
		log('######################################')
		zk.delete('node%d' % i)
		time.sleep(1)
	'''

	while True:
		log('running...')
		time.sleep(10)

	log('done')


