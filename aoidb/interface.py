import socket,asyncio
import os,sys
import struct
import json
import signal

from time import time,ctime,perf_counter_ns
from pickle import dumps,loads
from traceback import format_exc
from subprocess import Popen

from ._server_functions import *
from ._client_functions import recv_msg,send_with_len
from .database import AoiDB, AoiDB2
from .client import DataBaseClient


    
def run_server(server):
  loop = asyncio.get_event_loop()
  loop.create_task(asyncio.start_server(server.handle_client, '127.0.0.1', server.port))
  loop.run_forever()

class InterfaceServer():
  def __init__(self):
    self._start_port = 60000
    self._database_process = {}
    self._clients = {}

  def create_new(self, name='', version=2):
    if name in self._clients:
      return [0,'already exist']
    now_port = self._start_port+len(self._database_process)
    commands = ''"from aoidb.server import run_server; run_server({})"''
    config = {
      "database_option": {
        "name": name,
        "path": ""
      },
      "IP": "127.0.0.1",
      "Port": now_port,
      "version": 2
	  } 
    self._database_process[name] = Popen(['python','-c',commands.format(json.dumps(config,ensure_ascii=False))])
    self._clients[name] = DataBaseClient(port = now_port)
    return [0,'success']

  async def handle_request(self, request, peername):
    rtype, target, method, (args, kwargs) = request
    
    try:
      if rtype=='server':
        return [0, getattr(self, method)(*args,**kwargs)]
      elif rtype=='database':
        if target not in self._clients:
          return [2, 'the database is not in this server.']
        return [0, getattr(self._clients[target], method)(*args,**kwargs)]
      else:
        return [2, 'Requests Error: No command match']
    except Exception as e:
      err = format_exc()
      return [1, err]

  async def handle_client(self, reader, writer):
    ip, port = writer._transport.get_extra_info('peername')
    peername = f'{ip}:{port}'
    print(f'[{ctime()}]{peername}: connected')

    while True:
      try:
        request = (await recv(reader))
      except ConnectionResetError:
        break
      if not request:
        break
      
      response = await self.handle_request(request, peername)
      if response[0]==1:
        print(f'[{ctime()}]{peername}: Error Occured \n{log_error(response[1])}')
      elif response[0]==2:
        print(f'[{ctime()}]{peername}: Error Occured \n{response[1]}')
      await send(writer, response)

      try:
        await writer.drain()
      except ConnectionAbortedError:
        break
    print(f'[{ctime()}]{peername}: disconnected')
    writer.close()

  def run_server(self, IP, PORT):
    loop = asyncio.get_event_loop()
    loop.create_task(asyncio.start_server(self.handle_client, IP, PORT))
    loop.run_forever()


class InterfaceClient:
  def server_method(func):
    def function(*args,**kwargs):
      message = func(*args,**kwargs)
      if message[0]:
        log_error(message[1])
        return None
      else:
        return message[1]
    return function

  def __init__(self, ip='127.0.0.1', port=12345):
    self.HOST = ip
    self.PORT = port

    self._now_focus = ''
    self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.client.connect((ip, port))
  
  def __getitem__(self, key):
    return self.get('id',key)    

  def send(self, rtype, target, request, data=None):
    send_with_len(self.client, [rtype, target, request, data])

  def close(self):
    self.client.close()

  def lock(self):
    self.send('database', self._now_focus, 'lock', [[],{}])
    message = recv_msg(self.client)

  def unlock(self):
    self.send('database', self._now_focus, 'unlock', [[],{}])
    message = recv_msg(self.client)

  @server_method
  def create_new(self,name):
    self.send('server', self._now_focus, 'create_new', [[name],{}])
    return recv_msg(self.client)

  @server_method
  def ping(self):
    start = perf_counter_ns()
    self.send('database', self._now_focus, 'ping', [[],{}])
    message = recv_msg(self.client)
    cost = (perf_counter_ns()-start)/2/1000000
    return [message[0], '{}ms'.format(str(cost)[:5])]

  @server_method
  def speed_test(self):
    start = time()
    self.send('database', self._now_focus, 'speed_test', [[b' '*1000000*30],{}])
    message = recv_msg(self.client)
    cost = time()-start
    speed = str(30/cost*8)[:5]

    return [message[0], f'{speed}Mbps']
  
  @server_method
  def __str__(self):
    self.send('database', self._now_focus, '__str__', [[],{}])
    return recv_msg(self.client)
    
  @server_method
  def get_col(self):
    self.send('database', self._now_focus, 'col', [[],{}])
    return recv_msg(self.client)
  
  @server_method
  def save(self, path=''):
    self.send('database', self._now_focus, 'save', [[path], {}])
    return recv_msg(self.client)
  
  @server_method
  def get(self, **kwargs):
    self.send('database', self._now_focus, 'get', [[], kwargs])
    return recv_msg(self.client)
  
  @server_method
  def add_data(self, **kwargs):
    self.send('database', self._now_focus, 'add_data', [[],kwargs])
    return recv_msg(self.client)
  
  @server_method
  def delete(self, node):
    self.send('database', self._now_focus, 'delete', [[node],{}])
    return recv_msg(self.client)

  @server_method
  def delete_by_value(self, **kwargs):
    self.send('database', self._now_focus, 'delete_by_value', [[],kwargs])
    return recv_msg(self.client)

  @server_method
  def change_value(self, id, **kwargs):
    self.send('database', self._now_focus, 'change_value', [[id],kwargs])
    return recv_msg(self.client)

  @server_method
  def add_col(self, col, data_type):
    self.send('database', self._now_focus, 'add_col', [[col, data_type],{}])
    return recv_msg(self.client)

  @server_method
  def del_col(self, col):
    self.send('database', self._now_focus, 'del_col', [[col],{}])
    return recv_msg(self.client)
    
  @server_method
  def create_index(self, col):
    self.send('database', self._now_focus, 'create_index', [[col],{}])
    return recv_msg(self.client)
    
  @server_method
  def delete_index(self, col):
    self.send('database', self._now_focus, 'delete_index', [[col],{}])
    return recv_msg(self.client)