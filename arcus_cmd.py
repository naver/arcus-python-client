#!/usr/local/bin/python3

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
import socket

from kazoo.client import KazooClient
import kazoo
from optparse import OptionParser
import paramiko


lists = []


class arcus_node:
	def __init__(self, ip, port, name, code):
		self.ip = ip
		self.port = port 
		self.name = name
		self.code = code

	def __repr__(self):
		if self.name and self.code:
			return '[%s:%s-(%s,%s)]' % (self.ip, self.port, self.name, self.code)
		elif self.name:
			return '[%s:%s-(%s)]' % (self.ip, self.port, self.name)
		elif self.code:
			return '[%s:%s-(%s)]' % (self.ip, self.port, self.code)

		return '[%s:%s]' % (self.ip, self.port)

	def do_arcus_command(self, command):
		tn = telnetlib.Telnet(self.ip, self.port)
		tn.write(bytes(command + '\n', 'utf-8'))
		result = tn.read_until(bytes('END', 'utf-8'))
		result = result.decode('utf-8');
		tn.write(bytes('quit\n', 'utf-8'))
		tn.close()
		return result;


class zookeeper:
	def __init__(self, address):
		self.address = address
		self.zk = KazooClient(address)
		self.zk.start()

	def get_arcus_cache_list(self):
		children = self.zk.get_children('/arcus/cache_list/')
		return children

	def get_arcus_node_of_code(self, code, server):
		children = self.zk.get_children('/arcus/cache_list/' + code)

		ret = []
		for child in children:
			addr, name = child.split('-')
			ip, port = addr.split(':')

			if server != '' and (server != ip and server != name):
				continue # skip this

			node = arcus_node(ip, port, name, None)
			ret.append(node)

		return ret


	def get_arcus_node_of_server(self, addr):
		ip = socket.gethostbyname(addr)
		children = self.zk.get_children('/arcus/cache_server_mapping/')

		ret = []
		for child in children:
			l = len(ip)
			if child[:l] == ip:
				code = self.zk.get_children('/arcus/cache_server_mapping/' + child)
				ip, port = child.split(':')
				ret.append(arcus_node(ip, port, None, code[0]))
		return ret


def do_ssh_command(addr, command):
	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(addr)

	stdin, stdout, stderr = ssh.exec_command(command)
	for line in stdout.readlines():
		sys.stdout.write(line)
	ssh.close()


if __name__ == '__main__':
	usage = "usage: %prog [options]"
	parser = OptionParser(usage=usage, version="%prog 1.0")
	parser.add_option('-f', '--file', dest='file', default='', help='zookeeper address lists file')
	parser.add_option('-a', '--address', dest='address', default='', help='zookeeper address')
	parser.add_option('-s', '--service', dest='service', default='', help='service code')
	parser.add_option('-c', '--command', dest='command', default='', help='arcus command')
	parser.add_option('-n', '--node', dest='node', default='', help='node address or ip')
	parser.add_option('-x', '--ssh_command', dest='ssh_command', default='', help='ssh command execution')
	parser.add_option('-i', '--i', dest='info', default=False, help='memory, maxconns info', action='store_true')

	(options, args) = parser.parse_args()

	if options.file:
		fh = open(options.file)
		addresses = fh.readlines()
	else:
		addresses = [options.address]


	for address in addresses:
		try:
			print ('\n\n## Zookeeper address %s' % address)
			zoo = zookeeper(address)

			if options.service:
				lists = zoo.get_arcus_node_of_code(options.service, options.node)
			elif options.node:
				lists = zoo.get_arcus_node_of_server(options.node)
			else:
				lists = zoo.get_arcus_cache_list()
				print (lists)
				continue

		except Exception as e:
			print(e)
			# not found
			continue

		break


	lists.sort(key = lambda x: x.ip+x.port)
	for node in lists:
		print(node)

	if options.ssh_command:
		do_ssh_command(lists[0].ip, options.ssh_command)


	if options.command:
		for node in lists:
			try:
				result = node.do_arcus_command(options.command)
				print ('%s\t\t%s - %s' % (node, options.command, result))
			except Exception as e:
				print ('%s\t\tFAILED!!' % (node))
				print(e)
				

	if options.info:
		if options.node:
			print('===================================================================================')
			print ('[%s] system memory' % lists[0].ip);
			do_ssh_command(lists[0].ip, 'free') # run once
			print('-----------------------------------------------------------------------------------')


		re_limit = re.compile("STAT limit_maxbytes ([0-9]+)")
		re_bytes = re.compile("STAT bytes ([0-9]+)")
		re_curr_conn = re.compile("STAT curr_connections ([0-9]+)")
		re_maxconns = re.compile("maxconns ([0-9]+)")

		last_node = None
		for node in lists:
			try:
				if options.service and last_node != node.ip:
					print('===================================================================================')
					print ('[%s] system memory' % node.ip);
					do_ssh_command(node.ip, 'free') # run every server
					last_node = node.ip
					print('-----------------------------------------------------------------------------------')

				result = node.do_arcus_command('stats')
				m_limit = re_limit.search(result)
				m_bytes = re_bytes.search(result)
				m_curr_conn = re_curr_conn.search(result)

				result = node.do_arcus_command('config maxconns')
				m_maxconns = re_maxconns.search(result)

				if m_limit == None or m_bytes == None or m_maxconns == None or m_curr_conn == None:
					print ('%s\t\tstats failed!!' % (node))
					continue
				
				limit = int(m_limit.groups()[0]) / 1024 /  1024
				used = int(m_bytes.groups()[0]) / 1024 / 1024
				curr_conn = int(m_curr_conn.groups()[0])
				maxconns = int(m_maxconns.groups()[0])

				print ('%s\t\tMEM: (%d/%d) %f%%, CONN: (%d/%d)' % (node, used, limit, used/limit*100, curr_conn, maxconns))

			except Exception as e:
				print ('%s\t\tFAILED!!' % (node))
				print(e)
		print('===================================================================================')


