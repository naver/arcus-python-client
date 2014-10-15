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
 

import sys,os,socket,re

from optparse import OptionParser
import paramiko

from arcus_util import zookeeper
from arcus_util import arcus_node

from kazoo.client import KazooClient
import kazoo



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
	parser.add_option('', '--ssh_command_file', dest='ssh_command_file', default='', help='ssh command execution from file')
	parser.add_option('-i', '--i', dest='info', default=False, help='memory, maxconns info', action='store_true')
	parser.add_option('', '--dump_script', dest='dump_script', default=False, help='dump start script', action='store_true')
	parser.add_option('', '--vpn_remap', dest='vpn_remap', default='', help='read ip remap file for vpn network')

	(options, args) = parser.parse_args()

	if options.file:
		fh = open(options.file)
		addresses = fh.readlines()
	else:
		addresses = [options.address]

	remap = {}
	if options.vpn_remap:
		fh = open(options.vpn_remap)
		lines = fh.readlines()
		for line in lines:
			if line.strip() == '' or line[0] == '#':
				continue
		
			fr, to = line.split()
			remap[fr] = to

	lists = []

	for address in addresses:
		if address.strip() == '' or address[0] == '#':
			continue
		

		try:
			if len(remap) > 0:
				addr, port = address.split(':')
				ip = socket.gethostbyname(addr)
				if ip in remap:
					print('## zookeeper ip remap %s -> %s for address %s' % (ip, remap[ip], addr))
					address = '%s:%s' % (remap[ip], port)
				
			zoo = zookeeper(address)

			if options.service:
				list = zoo.get_arcus_node_of_code(options.service, options.node)
				if len(list) > 0:
					print ('\n\n## Zookeeper address %s' % address)
			elif options.node:
				list = zoo.get_arcus_node_of_server(options.node)
				if len(list) > 0:
					print ('\n\n## Zookeeper address %s' % address)
			else:
				print ('\n\n## Zookeeper address %s' % address)
				list = zoo.get_arcus_cache_list()
				print (list)
				list = []
				continue

			if options.dump_script: # record zookeeper address
				for node in list:
					node.zk_addr = address
				

		except kazoo.exceptions.NoNodeError:
			# not found
			continue

		lists = lists + list


	lists.sort(key = lambda x: x.ip + ":" + x.port)
	for node in lists:
		if node.ip in remap:
			print('## vpn remap %s -> %s' % (node.ip, remap[node.ip]))
			node.ip = remap[node.ip]
			
		print(node)

	if options.ssh_command_file:
		fh = open(options.ssh_command_file)
		options.ssh_command = fh.read()

	if options.ssh_command:
		prev_ip = ''
		for node in lists:
			if prev_ip != node.ip: # run once per machine
				print ('## run ssh command, [%s] %s' % (node.ip, options.ssh_command))
				do_ssh_command(node.ip, options.ssh_command)
				prev_ip = node.ip

	if options.command:
		for node in lists:

			try:
				result = node.do_arcus_command(options.command)
				print ('%s\t\t%s - %s' % (node, options.command, result))
			except Exception as e:
				print ('%s\t\tFAILED!!' % (node))
				print(e)
				

	if options.info or options.dump_script:
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

		total_used = 0
		total_limit = 0
		for node in lists:
			try:
				if options.service and last_node != node.ip:
					if last_node != None:
						print ('TOTAL MEM: (%d/%d) %f%%' % (total_used, total_limit, total_used/total_limit*100))
						total_used = total_limit = 0

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
				total_used = total_used + used;
				total_limit = total_limit + limit;

			except Exception as e:
				print ('%s\t\tFAILED!!' % (node))
				print(e)

			if options.dump_script:
				file_name = 'start_mem_%s.sh' % node.code
				script_fh = open(file_name, 'a')
				if os.path.getsize(file_name) == 0:
					script_fh.write('#!/bin/bash\n')
					os.chmod(file_name, 0o755)

				start_script = '/home1/irteam/apps/arcus/arcus/bin/memcached -v -o 60 -r -m%d -R5 -p %s -d -c %d -U 0 -D: -b 8192 -u irteam -t 6 -E /home1/irteam/apps/arcus/arcus/lib/default_engine.so -X /home1/irteam/apps/arcus/arcus/lib/syslog_logger.so -X /home1/irteam/apps/arcus/arcus/lib/ascii_scrub.so -z %s\n' % (limit, node.port, maxconns, node.zk_addr)

				script_fh.write(start_script)
				
				

		print ('TOTAL MEM: (%d/%d) %f%%' % (total_used, total_limit, total_used/total_limit*100))
		print('===================================================================================')


