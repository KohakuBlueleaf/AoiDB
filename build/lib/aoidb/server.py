import socket,asyncio
import os,sys
import struct
import json
from time import time,ctime
from pickle import dumps,loads
from traceback import format_exc
from ._server_functions import *
from .database import AoiDB


lock = ''
async def wait_lock(peername):
	global lock
	while lock:
		await asyncio.sleep(0.05)

DB = DB_function_list = None
async def handle_request(request,peername):
	global DB,DB_function_list,lock

	request, (args, kwargs) = request
	try:
		if request=='ping':
			return [0, None]
		
		elif request=='speed_test':
			return [0, None]
		
		if lock!=peername:
			await wait_lock(peername)
		
		if request=='lock':
			lock = peername
			print(f'\n[{ctime()}]{peername}: Lock Now')
			return [0,None]

		elif request=='unlock':
			print(f'[{ctime()}]{peername}: Unlock Now\n')
			lock = ''
			return [0,None]

		elif request in DB_function_list:
			print(f'[{ctime()}]{peername}: Operation({request})')
			return [0, getattr(DB,request)(*args, **kwargs)]
		
		else:
			return [2, 'Requests Error: No command match']
	except Exception as e:
		err = format_exc()
		return [1, err]

async def handle_client(reader, writer):
	global lock
	ip,port = writer._transport.get_extra_info('peername')
	peername = f'{ip}:{port}'
	print(f'[{ctime()}]{peername}: connected')

	while True:
		try:
			request = (await recv(reader))
		except ConnectionResetError:
			break
		if not request:
			break
		
		response = await handle_request(request,peername)
		if response[0]:
			print(f'[{ctime()}]{peername}: Error Occured \n{log_error(response[1])}')
		await send(writer, response)

		try:
			await writer.drain()
		except ConnectionAbortedError:
			break
	if lock==peername:
		lock=''
	print(f'[{ctime()}]{peername}: disconnected')
	writer.close()

def run_server(config=''):
	global DB,DB_function_list

	if type(config)!=str:
		config = config
	elif config[-5:]=='.json':
		with open(config, 'r', encoding='utf-8') as f:
			config = json.load(f)
	elif config!='':
		config = json.loads(config)
	else:
		config = {"database_option": {
								"name": "",
								"path": ""
							},
							"IP": "127.0.0.1",
							"Port": 22222}
	
	path = config['database_option']['path']

	DB = AoiDB(config['database_option']['name'])
	if path:
		DB.load(path)
	DB_function_list = set(dir(DB))

	IP, Port = config['IP'], config['Port']
	print(f'{DB.name}: run on {IP}:{Port}')
	loop = asyncio.get_event_loop()
	loop.create_task(asyncio.start_server(handle_client, IP, Port))
	loop.run_forever()

def save_config(DB):
	if not DB.path:
		raise ValueError('The DB object should have path')
	config = {}
	config['database_option'] = {}
	config['database_option']['name'] = DB.name
	config['database_option']['path'] = DB.path
	config['IP'] = '127.0.0.1'
	config['Port'] = 12345

	with open('./config.json','w',encoding='utf-8') as f:
		f.write(json.dumps(config,indent=2,ensure_ascii=False))