from socket import *
from time import sleep
from pickle import dump,load
from AoiDB import *
from Functions import *
		
		
class AoiServer:
	def __init__(self, host, port):
		self.DB = AoiPy()
		
		self.host = host
		self.port = port
		
		self.server = socket(AF_INET, SOCK_STREAM)
		self.server.bind((host,port))
		self.server.setsockopt(SOL_SOCKET,SO_REUSEADDR,1)
	
	def send(self, value):
		self.conn.sendall(value+b'##end##')
		self.conn.recv(1024)
	
	def recv(self):
		res = b''
		i=0
		while True:
			r = self.conn.recv(65536)
			res += r
			i += 1
			print(f'recv: {i}', end='\r')
			if res[-7:]==b'##end##' or not r:
				sleep(0.001)
				self.conn.send(b'ok')
				return res[:-7]
	
	def main_loop(self):
		self.server.listen(1)
		print(f'start listen at {self.host}:{self.port}')
		
		while True:
			self.conn, addr = self.server.accept()
			print('Connection created')
			
			while not self.conn.fileno() == -1:
				message = decode(self.recv())
				if message: print(message)
				
				if message == '##close':
					self.conn.close() 	
					print('conn close')
				elif message == '##load':
					self.load()
				elif message == '##get_str':
					self.get_str()
				elif message == '##get':
					self.get()
			
			print('Disconnect')
	
	def load(self):
		name = decode(self.recv())
		self.DB.load(f'./{name}.aoi')
		self.send(encode('load success'))
	
	def get_str(self):
		res = str(self.DB)
		self.send(encode(res))
		
	def get(self):
		key, mode = decode(self.recv()).split()
		value = ByteObject(self.recv()).load()
		
		res = self.DB.get(key, value, mode)
		
		if res:
			self.send(ByteObject().dump(res))
		else:
			self.send(b'Error')
	
server = AoiServer('', 9022)
server.main_loop()

