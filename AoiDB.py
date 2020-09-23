import os,sys
import pickle
from BpTree import BpTree
from time import time
from copy import deepcopy as dc

sys.setrecursionlimit(10000000)


def get_str(s, limit=10):
	'''
	自製format用來限制長度並判斷全半形
	'''
	s = str(s)
	res = ''
	amount = 0
	for i in s:
		amount += 1 if ord(i)<12288 else 2
		if amount>limit:
			amount -= 1 if ord(i)<12288 else 2
			break
		res += i
	
	res = ' '*(limit-amount) + res
	return res

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
	
	def __getitem__(self, key:str):
		return self.data.get(key,None)
		
	def __setitem__(self, key:str, value):
		if key not in self.data: 
			raise KeyError('Key is not in data')
		self.temp[key] = self.data[key]
		self.data[key] = value
	

class AoiDB:
	class DataList:
		def __init__(self, DB, datalist):
			self.DB = DB
			self.data = data_list
		
		def __str__(self):
			out = 'DataList('
			for i in self.data:
				out += str(i)
				out += ','
			
			return out[:-1] + ')'	
		
		def __getitem__(key):
			return self.data[key]
		
		def __iter__(self):
			for i in self.data:
				yield data
		
		def save_change(self):
			for data in self.data:
				self.DB.save_change(data)
		
	def __init__(self, name=''):
		self.name = name
		self.path = ''
		
		self.data = []
		
		self.column = []
		self.type = {}
		self.index = {}
		self.cmp = {}
		
		self.id_list = BpTree(12)
		self.idmax = 0
	
	def __iter__(self):
		for i in self.data:
			yield i
	
	def show(self, sort_by=''):
		out = f'BlueData_{self.name }:\n'
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
				res = sorted(self.data, key=key)
				
			for i in res:
				out += spliter
				out += '{}│'.format(get_str(i.id))
				for j in self.column:
					out += '{}│'.format(get_str(i[j]))
		else:
			for i in self.data:
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
	
	def save(self,path=''):
		if path[:2] != './' or path[0] !='/':
			path = './'+path
			path += '.aoi'
			
		all_data = (self.name, self.type, self.data, self.column, self.index, self.id_list, self.idmax)
		with open(path if path else self.path, 'wb') as f:
			pickle.dump(all_data, f)
	
	def load(self, path):
		if not self.path:
			self.path = path
		
		with open(path, 'rb') as f:
			all_data = pickle.load(f)
		
		self.name, self.type, self.data, self.column, self.index, self.id_list, self.idmax = all_data
		
	def get(self, key:str, target, mode='=', index=False):
		if key=='id':
			return self.id_list[target]

		if mode=='=':
			if index and key in self.index:
				return self.index[key][target]
			
			res = [i for i in self.data if i[key]==target]
			return res
			
		elif mode=='<':
			if index and key in self.index:
				end = self.index[key][target]
				if end:
					res = []
					for i in self.index[key]:
						if i==end: return res
						res += i

			res = [i for i in self.data if i[key]<target]
			return res
			
		elif mode=='>':
			if index and key in self.index:
				start, v_i = self.index[key].get(target, None)
				if start:
					if start[v_i]==self.index[key][target]:
						v_i += 1
						v_i %= len(start.value)
					
					res = start.value[v_i:]
					while start:
						res += start.value
						start = start.next
					
					return res
				else:
					return []
			
			res = [i for i in self.data if i[key]>target]
			return res
	
	
	def add_data(self, **kwargs):
		new = Data(self.idmax)
		
		self.id_list[self.idmax] = new
		self.data.append(new)
		self.idmax += 1
		
		for i in self.column:
			new.data[i] = self.type[i]()
			if i in self.index:
				self.index[i][new.data[i]].append(new)
		
		for key,value in kwargs.items():
			new[key] = value
			
		return new
	
	def delete(self, node:Data):
		self.data.remove(node)
		
		for key,tree in self.index.items():
			tree[node[key]].remove(node)
		
		self.id_list[node.id] = None
	
	def delete_by_value(self, key:str, value, mode='='):
		res = self.get(key, value, mode, index=(key in self.index))
		for i in res:
			self.delete(i)
		
	def change_value(self, key, value, mode='=', **kwargs):
		res = self.get(key, value, mode, True)
		
		for target in res:
			for key,value in kwargs.items():
				before = dc(target[key])
				target[key] = value
				
				if key in self.index:
					self.index[key][before].remove(target)
					if value in self.index[key]:
						self.index[key][value].append(target)
					else:
						self.index[key][value] = [target]
				
	def save_change(self, node:Data):
		for key in self.column:
			if self.type[key] != type(node[key]):
				raise TypeError(f"{key} should be {self.type[key]}")
				
		for key in self.index:
			if node.temp[key] != node[key]:
				self.index[key][node.temp[key]].remove(node)
				
				value = node[key]
				if value in self.index[key]:
					self.index[key][value].append(node)
				else:
					self.index[key][value] = [node]
				
				node.temp[key] = node[key]
	
	
	def list_col(self):
		print(self.type.index)

	def add_col(self, col, data_type):
		self.type[col] = type(data_type)
		self.column.append(col)
		
		for i in self.data:
			i.data[col] = type(data_type)()
	
	def del_col(self, col):
		del self.type[col]
		self.column.remove(col)
		
		for i in self.data:
			del i.data[col]
		
		if col in self.index:
			del self.index[col]
	
	
	def create_index(self, key:str):
		self.index[key] = BpTree(12)
		self.index[key][self.type[key]()] = []
		index = self.index[key]
		
		for i in self.data:
			value = i[key]
			if value in index:
				index[value].append(i)
			else:
				index[value] = [i]
	
	def delete_index(self, key:str):
		del self.index[key]
