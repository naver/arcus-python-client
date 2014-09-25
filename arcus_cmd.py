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
 

import sys
import telnetlib
import re

from kazoo.client import KazooClient
import kazoo
from optparse import OptionParser
import paramiko


lists = []

def get_arcus_node_list(addr, code):
	zk = KazooClient(addr)
	zk.start()
	children = zk.get_children('/arcus/cache_list/' + code)

	if code == '':
		return children

	ret = []
	for child in children:
		addr, name = child.split('-')
		ip, port = addr.split(':')
		ret.append((name, port, ip))

	return ret


def get_arcus_service_list(addr, ip):
	zk = KazooClient(addr)
	zk.start()
	children = zk.get_children('/arcus/cache_server_mapping/')

	ret = []
	for child in children:
		l = len(ip)
		if child[:l] == ip:
			service = zk.get_children('/arcus/cache_server_mapping/' + child)
			ip, port = child.split(':')
			ret.append((ip, port, service[0]))
	return ret

def do_ssh_command(addr, command):
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(addr)

	stdin, stdout, stderr = ssh.exec_command(command)
	for line in stdout.readlines():
		sys.stdout.write(line)
	ssh.close()

def do_arcus_command(ip, port, command):
	tn = telnetlib.Telnet(ip, port)
	tn.write(bytes(command + '\n', 'utf-8'))
	result = tn.read_until(bytes('END', 'utf-8'))
	result = result.decode('utf-8');
	tn.write(bytes('quit\n', 'utf-8'))
	return result;


if __name__ == '__main__':
	usage = "usage: %prog [options]"
	parser = OptionParser(usage=usage, version="%prog 1.0")
	parser.add_option('-a', '--address', dest='address', default='', help='zookeeper address')
	parser.add_option('-s', '--service', dest='service', default='', help='service code')
	parser.add_option('-c', '--command', dest='command', default='', help='command')
	parser.add_option('-n', '--node', dest='node', default='', help='node ip')
	parser.add_option('-x', '--ssh_command', dest='ssh_command', default='', help='ssh command execution')
	parser.add_option('-m', '--memory', dest='memory', default=False, help='memory info', action='store_true')

	(options, args) = parser.parse_args()

	if options.service and options.node:
		print('-s(--service) and -n(--node) is exclusive')
		exit()

	if options.service:
		lists = get_arcus_node_list(options.address, options.service)
	elif options.node:
		lists = get_arcus_service_list(options.address, options.node)
	else:
		lists = get_arcus_node_list(options.address, '') # print all cache_list


	lists.sort()

	print (lists)

	if options.ssh_command:
		do_ssh_command(lists[0][0], options.ssh_command)


	if options.command:
		for node in lists:
			try:
				result = do_arcus_command(node[0], node[1], options.command)
				print ('[%s:%s(%s)]\t\t%s - %s' % (node[0], node[1], node[2], options.command, result))
			except Exception as e:
				print ('[%s:%s(%s)]\t\tFAILED!!' % (node[0], node[1], node[2]))
				print(e)
				

	if options.memory:
		if options.node:
			print('===============================================================')
			print ('[%s] system memory' % lists[0][0]);
			do_ssh_command(lists[0][0], 'free') # run once
			print('---------------------------------------------------------------')


		re_limit = re.compile("STAT limit_maxbytes ([0-9]+)")
		re_bytes = re.compile("STAT bytes ([0-9]+)")

		last_node = None
		for node in lists:
			try:
				if options.service and last_node != node[0]:
					print('===============================================================')
					print ('[%s] system memory' % node[0]);
					do_ssh_command(node[0], 'free') # run every server
					last_node = node[0]
					print('---------------------------------------------------------------')

				result = do_arcus_command(node[0], node[1], 'stats\n')

				m_limit = re_limit.search(result)
				m_bytes = re_bytes.search(result)

				if m_limit == None or m_bytes == None:
					print ('[%s:%s(%s)]\t\tstats failed!!' % (node[0], node[1], node[2]))
					continue
				
				limit = int(m_limit.groups()[0]) / 1024 /  1024
				used = int(m_bytes.groups()[0]) / 1024 / 1024

				print ('[%s:%s(%s)]\t\t(%d/%d) %f%%)' % (node[0], node[1], node[2], used, limit, used/limit*100))

			except Exception as e:
				print ('[%s:%s(%s)]\t\tFAILED!!' % (node[0], node[1], node[2]))
				print(e)
		print('===============================================================')


