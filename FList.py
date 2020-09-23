import os,sys
from pickle import load,dump
from time import time

sys.setrecursionlimit(10000000)

class FList:
	def __init__(self, name ,path='./'):
		self.id = -1
		self.name = name
		self.path = path if path[-1]=='/' else path+'/'
		self.file_list = []
		
		if not os.path.isdir(self.path):
			os.mkdir(path)
		if not os.path.isdir(self.path+name):
			os.mkdir(path+name)
		
		self.path += name+'/'
	
	def __str__(self):
		out = 'Flist['
		
		for i in self.file_list:
			with open(i, 'rb') as f:
				value = load(f)
				out += '{}, '.format(value)

		return out[:-1]+']'
			
	def __setitem__(self, key, value):
		with open(self.file_list[key], 'wb') as f:
			dump(value, f)
		
	def __getitem__(self, key):
		with open(self.file_list[key], 'rb') as f:
			return load(f)
	
	def __iter__(self):
		for i in self.file_list:
			with open(i, 'rb') as f:
				value = load(f)
			
			yield value
	
	def __len__(self):
		return len(self.file_list)
	
	def append(self, value):
		self.id += 1
		self.file_list.append(self.path+str(self.id))
		
		with open(self.file_list[-1], 'wb') as f:
			dump(value, f)
	
	def insert(self, key, value):
		self.id += 1
		self.file_list.insert(key, self.path+str(self.id))
		
		with open(self.file_list[key], 'wb') as f:
			dump(value, f)
	
	def remove_key(self, key):
		name = self.file_list[key]
		self.file_list.remove(name)
		os.remove(name)
	
	def remove(self, value):
		name = self.file_list[self.index(value)]
		self.file_list.remove(name)
		os.remove(name)
		
	def index(self, value):
		for i in range(len(self.file_list)):
			with open(self.file_list[i], 'rb') as f:
				val = load(f)
			
			if value==val:
				return i

if __name__=='__main__':
	a = FList('test')
	b = []
	res = [i**0.5 for i in range(10000)]
	
	T0 = time()
	for i in res:
		a.append(i)
	T1 = time()
	for i in res:
		b.append(i)
	T2 = time()
	
	print('FList: {}'.format(str(T1 - T0)[:5]))
	print('List : {}'.format(str(T2 - T1)[:5]))



