import socket
from time import time,perf_counter_ns
from pickle import dumps,loads
import asyncio

from ._client_functions import *


class DataBaseClient:
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

  def send(self, request, data=None):
    send_with_len(self.client, [request,data])

  def close(self):
    self.client.close()

  def lock(self):
    self.send('lock', [[],{}])
    message = recv_msg(self.client)

  def unlock(self):
    self.send('unlock', [[],{}])
    message = recv_msg(self.client)

  @server_method
  def ping(self):
    start = perf_counter_ns()
    self.send('ping', [[],{}])
    message = recv_msg(self.client)
    cost = (perf_counter_ns()-start)/2/1000000
    return [message[0], '{}ms'.format(str(cost)[:5])]

  @server_method
  def speed_test(self):
    start = time()
    self.send('speed_test', [[b' '*1000000*30],{}])
    message = recv_msg(self.client)
    cost = time()-start
    speed = str(30/cost*8)[:5]

    return [message[0], f'{speed}Mbps']
  
  @server_method
  def __str__(self):
    self.send('__str__', [[],{}])
    return recv_msg(self.client)
    
  @server_method
  def get_col(self):
    self.send('col', [[],{}])
    return recv_msg(self.client)
  
  @server_method
  def save(self, path=''):
    self.send('save', [[path], {}])
    return recv_msg(self.client)
  
  @server_method
  def get(self, **kwargs):
    self.send('get', [[], kwargs])
    return recv_msg(self.client)
  
  @server_method
  def add_data(self, **kwargs):
    self.send('add_data', [[],kwargs])
    return recv_msg(self.client)
  
  @server_method
  def delete(self, node):
    self.send('delete', [[node],{}])
    return recv_msg(self.client)

  @server_method
  def delete_by_value(self, **kwargs):
    self.send('delete_by_value', [[],kwargs])
    return recv_msg(self.client)

  @server_method
  def change_value(self, id, **kwargs):
    self.send('change_value', [[id],kwargs])
    return recv_msg(self.client)

  @server_method
  def add_col(self, col, data_type):
    self.send('add_col', [[col, data_type],{}])
    return recv_msg(self.client)

  @server_method
  def del_col(self, col):
    self.send('del_col', [[col],{}])
    return recv_msg(self.client)
    
  @server_method
  def create_index(self, col):
    self.send('create_index', [[col],{}])
    return recv_msg(self.client)
    
  @server_method
  def delete_index(self, col):
    self.send('delete_index', [[col],{}])
    return recv_msg(self.client)

class AsyncClient:
  def server_method(func):
    async def function(*args,**kwargs):
      message = await func(*args,**kwargs)
      if message[0]:
        log_error(message[1])
        return None
      else:
        return message[1]
    return function

  def __init__(self, ip='127.0.0.1', port=12345):
    self.HOST = ip
    self.PORT = port

    self.writer = self.reader = None 

  async def connect(self):
    reader, writer = await asyncio.open_connection(self.HOST, self.PORT)
    self.writer = writer
    self.reader = reader
  
  async def close(self):
    self.writer.close()
    await self.writer.wait_closed()

  async def send(self, request, data=None):
    await a_send(self.writer, [request,data])

  async def lock(self):
    await self.send('lock', [[],{}])
    message = await a_recv(self.reader)

  async def unlock(self):
    await self.send('unlock', [[],{}])
    message = await a_recv(self.reader)

  @server_method
  async def ping(self):
    start = perf_counter_ns()
    await self.send('ping', [[],{}])
    message = await a_recv(self.reader)
    cost = (perf_counter_ns()-start)/2/1000000
    return [message[0], '{}ms'.format(str(cost)[:5])]

  @server_method
  async def speed_test(self):
    start = time()
    await self.send('speed_test', [[b' '*1000000*30],{}])
    message = await a_recv(self.reader)
    cost = time()-start
    speed = str(30/cost*8)[:5]

    return [message[0], f'{speed}Mbps']
  
  @server_method
  async def __str__(self):
    await self.send('__str__', [[],{}])
    return await a_recv(self.reader)
    
  @server_method
  async def get_col(self):
    await self.send('col', [[],{}])
    return await a_recv(self.reader)
  
  @server_method
  async def save(self, path=''):
    await self.send('save', [[path], {}])
    return await a_recv(self.reader)
  
  @server_method
  async def get(self, **kwargs):
    await self.send('get', [[], kwargs])
    return await a_recv(self.reader)
  
  @server_method
  async def add_data(self, **kwargs):
    await self.send('add_data', [[],kwargs])
    return await a_recv(self.reader)
  
  @server_method
  async def delete(self, node):
    await self.send('delete', [[node],{}])
    return await a_recv(self.reader)

  @server_method
  async def delete_by_value(self, **kwargs):
    await self.send('delete_by_value', [[],kwargs])
    return await a_recv(self.reader)

  @server_method
  async def change_value(self, id, **kwargs):
    await self.send('change_value', [[id],kwargs])
    return await a_recv(self.reader)

  @server_method
  async def add_col(self, col, data_type):
    await self.send('add_col', [[col, data_type],{}])
    return await a_recv(self.reader)

  @server_method
  async def del_col(self, col):
    await self.send('del_col', [[col],{}])
    return await a_recv(self.reader)
    
  @server_method
  async def create_index(self, col):
    await self.send('create_index', [[col],{}])
    return await a_recv(self.reader)
    
  @server_method
  async def delete_index(self, col):
    await self.send('delete_index', [[col],{}])
    return await a_recv(self.reader)
