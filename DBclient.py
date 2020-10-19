from socket import *
from time import sleep,time
from pickle import dump,load
from AoiDB import *
from Functions import *


class AoiClient:
	def __init__(self, host, port):
		self.client = socket(AF_INET, SOCK_STREAM)
		self.client.connect(('127.0.0.1',9022))
	
	def send(self, value):
		self.client.sendall(value+b'##end##')
		self.client.recv(1024)
	
	def recv(self):
		res = b''
		i = 0
		while True:
			r = self.client.recv(65536)
			res += r
			i += 1
			print(f'recv: {i}', end='\r')
			if res[-7:]==b'##end##' or not r:
				sleep(0.001)
				self.client.send(b'ok')
				return res[:-7]
	
	def close(self):
		self.send(b'##close')
		
	def load(self, name):
		self.send(b'##load')
		self.send(encode(name))
		print(decode(self.recv()))
	
	def get_str(self):
		self.send(b'##get_str')
		return decode(self.recv())
	
	def get(self, key, value, mode='='):
		self.send(b'##get')
		self.send(encode(key+' '+mode))
		self.send(ByteObject().dump(value))
		
		T0 = time()
		recv = self.recv()
		T1 = time()
		
		if recv!=b'Error':
			res = ByteObject(recv).load()
			return res
		else:
			return (None,)


cli = AoiClient('127.0.0.1', 9022)
cli.load('指對數表')
res = cli.get('num',50,'<')
print(res)
print(len(res))
cli.close()
