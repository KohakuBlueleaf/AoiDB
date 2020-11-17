import os,sys
import pickle
from time import time
from copy import copy,deepcopy
from ._bp_tree import BpTree
from ._functions import *

sys.setrecursionlimit(10000000)


class AoiDB:
	class Data:
		"""
		DB中的Data物件，儲存資料的最小單位
		"""
		def __init__(self, id:int):
			self.id = id
			self.temp = {}	#儲存上一次的資料，在save_change的時用來判斷有沒有修改
			self.data = {}	#儲存資料
			
		def __str__(self):
			length = len(self.data)
			first = '\n┌──────────'+'┬──────────'*length+'┐\n│'
			row1 = '        id│'
			middle = '\n├──────────'+'┼──────────'*length+'┤\n│'
			row2 = '{}│'.format(get_str(self.id))
			end = '\n└──────────'+'┴──────────'*length+'┘'
			
			for key,value in self.data.items():
				row1 += '{}│'.format(get_str(key))
				row2 += '{}│'.format(get_str(value))
			
			return first+row1+middle+row2+end
		
		__repr__ = __str__
		
		def copy(self,node):
			self.temp = node.temp
			self.data = node.data

		def __len__(self):
			return len(self.data)

		def __iter__(self):
			for i in self.data:
				yield i
		
		def items(self):
			return self.data.items()
		
		def keys(self):
			return self.data.keys()

		def values(self):
			return self.data.values()

		def __getitem__(self, key:str):
			return self.data.get(key,None)

		def __delitem__(self, key):
			del self.data[key]
			
		def __setitem__(self, key:str, value):
			if key not in self.data: 
				raise KeyError('Key is not in data')
			self.temp[key] = self.data[key]
			self.data[key] = value
		
	
	class DataSet:
		"""
		DataSet物件 當db須回傳多筆資料時使用此物件儲存
		"""
		def __init__(self, data):
			self.data = data
		
		def __str__(self):
			out = 'DataSet('
			for i in self.data:
				out += str(i)
				out += ','
			
			return out[:-1] + ')'	
		
		def __getitem__(self,key):
			return self.data[key]
		
		def __iter__(self):
			for i in self.data:
				yield i
		
		def __len__(self):
			return len(self.data)
		
		def __add__(self, other):
			return AoiDB.DataSet(self.data + other.data)


	def __init__(self, name=''):
		self.name = name
		self.path = ''
		
		self.all_data = []
		
		self.column = []
		self.type = {}
		self.index = {}
		self.cmp = {}
		self.command_list = []
		
		self.id_list = BpTree(12)
		self.idmax = 0
	
	def __iter__(self):
		for i in self.all_data:
			yield i
	
	def show(self, sort_by=''):
		out = f'AoiDB_{self.name }:\n'
		length = len(self.column)
		spliter = '\n├──────────'+'┼──────────'*length+'┤\n│'
		out += '┌──────────'+'┬──────────'*length+'┐\n│'
		
		out += '{:>10}│'.format('id')
		
		for i in self.column:
			out += '{}│'.format(get_str(i))
		
		if sort_by:
			if sort_by in self.index:
				res = sum([i for i in self.index[sort_by]],[])
			else:
				if sort_by in self.cmp:
					key = lambda i:self.cmp[sort_by](i[sort_by])
				else:
					key = lambda i:i[sort_by]
				res = sorted(self.all_data, key=key)
				
			for i in res:
				out += spliter
				out += '{}│'.format(get_str(i.id))
				for j in self.column:
					out += '{}│'.format(get_str(i[j]))
		else:
			for i in self.all_data:
				out += spliter
				out += '{}│'.format(get_str(i.id))
				for j in self.column:
					out += '{}│'.format(get_str(i[j]))
		
		out += '\n└──────────'+'┴──────────'*length+'┘'
		
		return out
	
	__str__ = show
	
	def __getitem__(self, key):
		if key in self.id_list:
			return self.id_list[key]
	
	def __contains__(self, id):
		return id in self.id_list
	
	def save(self,path='', abs_path=False):
		if path=='' and self.path=='':
			path = self.name

		if path[-4:]!='.aoi':
			path+='.aoi'
			
		all_data = (self.name, self.type, self.all_data, self.column, self.index, self.id_list, self.idmax)
		with open(path if path else self.path, 'wb') as f:
			pickle.dump(all_data, f)
	
	def load(self, path):
		if not self.path:
			self.path = path
		
		with open(path, 'rb') as f:
			all_data = pickle.load(f)
		
		self.name, self.type, self.all_data, self.column, self.index, self.id_list, self.idmax = all_data
		for i in self.all_data:
			i.temp = deepcopy(i.data)
	

	def get_by_id(self, id):
		return self.id_list[id]
	
	def get_e(self, key, target):
		if key in self.index:
			return self.DataSet(self.index[key].get(target,[]))
		else:
			return self.DataSet([i for i in self.all_data if i[key]==target])
	
	def get_l(self, key, target):
		return self.DataSet([i for i in self.all_data if i[key]<target])
	
	def get_g(self, key, target):
		return self.DataSet([i for i in self.all_data if i[key]>target])
	
	def get(self, key:str, target,mode='='):
		if key=='id':
			return self.get_by_id(target)
		elif mode=='=':
			return self.get_e(key, target)
		elif mode=='<':
			return self.get_l(key, target)
		elif mode=='>':
			return self.get_g(key, target)
			

	def add_data(self, **kwargs):
		new = self.Data(self.idmax)
		
		self.id_list[self.idmax] = new
		self.all_data.append(new)
		self.idmax += 1
		
		for key,value in kwargs.items():
			new.data[key] = value
			
		for i in self.column:
			if i not in new.data:
				new.data[i] = self.type[i]()
				
			if i in self.index:
				if new.data[i] in self.index[i]:
					self.index[i][new.data[i]].append(new)
				else:
					self.index[i][new.data[i]] = [new]
		
			new.temp[i] = new.data[i]
		
		return new
	
	def delete(self, node:Data):
		data = self.id_list[node.id]
		data.copy(node)
		node = data

		self.all_data.remove(node)
		
		for key,tree in self.index.items():
			tree[node[key]].remove(node)
		
		self.id_list[node.id] = None
	
	def delete_by_value(self, key:str, value, mode='='):
		res = self.get(key, value, mode)
		for i in res:
			self.delete(i)
	
	def change_value(self, id, **kwargs):
		target = self.id_list[id]
		for key,value in kwargs.items():
			before = deepcopy(target[key])
			target[key] = value
			
			if key in self.index:
				self.index[key][before].remove(target)
				if value in self.index[key]:
					self.index[key][value].append(target)
				else:
					self.index[key][value] = [target]
		self.command_list.append([id,kwargs])

	def col(self):
		return list(self.type.keys())

	def add_col(self, col, data_type):
		if col not in self.type:
			self.type[col] = type(data_type)
			self.column.append(col)
			
			for i in self.all_data:
				i.data[col] = type(data_type)()
	
	def del_col(self, col):
		del self.type[col]
		self.column.remove(col)
		
		for i in self.all_data:
			del i.data[col]
		
		if col in self.index:
			del self.index[col]
	

	def create_index(self, key:str):
		self.index[key] = BpTree(12)
		self.index[key][self.type[key]()] = []
		index = self.index[key]
		
		for i in self.all_data:
			value = i[key]
			if value in index:
				index[value].append(i)
			else:
				index[value] = [i]
	
	def delete_index(self, key:str):
		del self.index[key]
	
	def load_by_temp(self, path):
		with open(path,'rb') as f:
			data = pickle.load(f)
		col = data[0].keys()
		for i in col:
			self.add_col(i, type(data[0][i])())
		
		for i in data:
			new = self.add_data()
			for key,value in i.items():
				new[key] = value
			
			self.save_change(new)
