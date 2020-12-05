import os,sys
import socket
import struct
from time import time,perf_counter_ns
from pickle import dumps,loads

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
  def add_data(self, *args, **kwargs):
    self.send('add_data', [[],kwargs])
    return recv_msg(self.client)
  
  @server_method
  def delete(self, node):
    self.send('delete', [[node],{}])
    return recv_msg(self.client)

  @server_method
  def delete_by_value(self, key:str, value, mode='='):
    self.send('delete_by_value', [[key,value,mode],{}])
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
