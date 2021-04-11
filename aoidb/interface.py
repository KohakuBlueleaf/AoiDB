from asyncio.events import Handle
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
from .client import AsyncClient


    
def run_server(server):
  loop = asyncio.get_event_loop()
  loop.create_task(asyncio.start_server(server.handle_client, '127.0.0.1', server.port))
  loop.run_forever()

class InterfaceServer():
  def set_server_method(func):
    func.__is_server_method__ = 0
    return func

  def __init__(self,path=''):
    if path:
      if not os.path.isdir(path):
        os.mkdir(path)
      if not os.path.isdir(path+'/databases'):
        os.mkdir(path+'/databases')
    
    self.path = path
    self.name = ''
    self.ip = ''
    self.port = int()

    self.loop = asyncio.get_event_loop()
    self._methods = {}
    self._start_port = 60000
    self._database_process = {}
    self._clients = {}
    self._configs = {}
  
  @set_server_method
  async def create_new(self, name='', version=2):
    if name in self._clients:
      return [0,'already exist']
    now_port = self._start_port+len(self._database_process)

    if self.path:
      path = f"{self.path.rstrip('/')}/databases/{name}.aoi{'' if version==1 else 2}"
    else:
      path = ''
    
    config = {
      "database_option": {
        "name": name,
        "path": path,
      },
      "IP": "127.0.0.1",
      "Port": now_port,
      "version": 2
	  } 
    await self.create_from_config(config)
    return 'success'
  
  async def create_from_config(self, config):
    commands = ''"from aoidb.server import run_server; run_server({})"''
    name = config['database_option']['name']

    self._configs[name] = config
    self._database_process[name] = Popen(['python','-c',commands.format(json.dumps(config,ensure_ascii=False))])
    self._clients[name] = AsyncClient(port = config['Port'])
    
    await self._clients[name].connect()

  @set_server_method
  async def get_config(self):
    new_config = {
      'name': self.name,
      'ip': self.ip,
      'port': self.port,
      'path': self.path,
      'configs':{
        name:config for name, config in self._configs.items()
      }
    }
    config_str = json.dumps(new_config, indent=2, ensure_ascii=False)
    return config_str

  @set_server_method
  async def save_all(self,path=''):
    if not path:
      if not self.path:
        self.path = path = '.'
      else:
        path = self.path
    else:
      path = path.strip('/')
      self.path = path

    if self.path:
      old_path = self.path
    
    if not os.path.isdir(path):
      os.mkdir(path)
    if not os.path.isdir(f'{path}/databases'):
      os.mkdir(f'{path}/databases')
    
    
    for name, config in self._configs.items():
      version = config['version']
      old_database_path = config['database_option']['path']

      if not old_database_path:
        new_path = f"{path}/databases/{name}.aoi{'' if version==1 else 2}"
        config['database_option']['path'] = new_path
      else:
        new_path = f"{self.path}/{old_database_path.split(old_path)[-1]}"
        config['database_option']['path'] = new_path
      
      await self._clients[name].save(new_path)
    
    all_config = await self.get_config()
    with open(f'{path}/config.json','w') as f:
      f.write(all_config)

  async def handle_request(self, request, peername):
    rtype, target, method, (args, kwargs) = request
    
    try:
      if rtype=='server':
        func = getattr(self, method)
        if hasattr(func,'__is_server_method__'):
          return [0, await func(*args,**kwargs)]
        else:
          return [1, 'Requests Error: No command match']
      elif rtype=='database':
        if target not in self._clients:
          return [2, f'DatabaseError: the database "{target}" is not in this server.']
        return [0, await getattr(self._clients[target], method)(*args,**kwargs)]
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
    self.ip = IP
    self.port = PORT
    self.loop.create_task(asyncio.start_server(self.handle_client, IP, PORT))

    try:
      self.loop.run_forever()
    except KeyboardInterrupt:
      self.loop.stop()
  
  def run_with_config(self, config):
    self.ip = config['ip']
    self.port = config['port']
    self.name = config['name']
    self.path = config['path']

    for i in config['configs'].values():
      self.loop.create_task(self.create_from_config(i))
    self.loop.create_task(asyncio.start_server(self.handle_client, self.ip, self.port))

    try:
      self.loop.run_forever()
    except KeyboardInterrupt:
      self.loop.stop()


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

    self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.client.connect((ip, port))
  
  def __getitem__(self, key):
    return self.get('id',key)    

  def send(self, rtype, target, request, data=None):
    send_with_len(self.client, [rtype, target, request, data])

  def close(self):
    self.client.close()

  def get_reader(self, name):
    return SubClient(self.HOST, self.PORT, name)

  @server_method
  def create_new(self, name):
    self.send('server', '', 'create_new', [[name],{}])
    return recv_msg(self.client)

  @server_method
  def get_config(self):
    self.send('server', '', 'get_config', [[],{}])
    return recv_msg(self.client)

  @server_method
  def save_all(self, path=''):
    self.send('server', '', 'save_all', [[path],{}])
    return recv_msg(self.client)


class SubClient:
  def server_method(func):
    def function(*args,**kwargs):
      message = func(*args,**kwargs)
      if message[0]:
        log_error(message[1])
        return None
      else:
        return message[1]
    return function

  def __init__(self, ip, port, focus):
    self.HOST = ip
    self.PORT = port

    self._now_focus = focus
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