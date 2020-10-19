from random import shuffle
from time import time
import sys

class BpTree:
	def __init__(self,order):
		self.order = order
		self.root = Node(order)
	
	def __str__(self):
		root = self.root
		out = '|{}|'.format(','.join([str(i) for i in root.keys]))
		b_layer = 1
		
		if not root.leaf:
			out += '\n'
			queue = [(1,i) for i in root.value]
			while queue:
				layer,now = queue.pop(0)
				
				if layer!=b_layer:
					b_layer = layer
					out += '|\n'
				
				out += '|{}'.format(','.join([str(i) for i in now.keys]))
				
				if not now.leaf:
					for i in now.value:
						queue.append([layer+1, i])
			out += '|'
		
		return out
	
	def __iter__(self):
		now = self.root
		
		while not now.leaf:
			now = now.value[0]
			
		while now:
			for i in range(len(now.value)):
				yield now.keys[i]
			now = now.next
	
	def __getitem__(self,key):
		now = self.root
		
		while not now.leaf:
			if key>=now.keys[-1]:
				now = now.value[-1]
			else:
				for i in range(len(now.keys)):
					if key<now.keys[i]:
						now = now.value[i]
						break
		
		for i in range(len(now.keys)):
			if now.keys[i]==key:
				return now.value[i]
				
		return None
	
	def __setitem__(self, key, value):
		now = self.root
		
		while not now.leaf:
			if key>=now.keys[-1]:
				now = now.value[-1]
			else:
				for i in range(len(now.keys)):
					if key<now.keys[i]:
						now = now.value[i]
						break
		
		for i in range(len(now.keys)):
			if now.keys[i]==key:
				now.value[i]=value
				return 
		
		now.add_value(key, value)
	
	def __contains__(self,key):
		now = self.root
		
		while not now.leaf:
			if key>=now.keys[-1]:
				now = now.value[-1]
				continue
			for i in range(len(now.keys)):
				if key<now.keys[i]:
					now = now.value[i]
					break
		
		return key in now.keys
	
	def __len__(self):
		res = 0
		now = self.root
		
		while not now.leaf:
			now = now.value[0]
		
		while now:
			res += len(now.value)
			now = now.next
		
		return res
	
	def delete(self, key):
		now = self.root
		
		while not now.leaf:
			if key>=now.keys[-1]:
				now = now.value[-1]
			else:
				for i in range(len(now.keys)):
					if key<now.keys[i]:
						now = now.value[i]
						break
		
		now.delete_value(key)
	
	def size(self):
		res = 0
		queue = [self.root]
		
		while queue:
			now = queue.pop()
			if now.leaf:
				res += len(now.keys)
			else:
				queue += now.value
				res += len(now.keys)
		
		return res
		
	def items(self):
		def foo(self):
			now = self.root
			
			while not now.leaf:
				now = now.value[0]
				
			while now:
				for i in range(len(now.value)):
					yield (now.keys[i], now.value[i])
				now = now.next
		return foo(self)
	
	
	def get(self,key,default=0):
		now = self.root
		
		while not now.leaf:
			if key>=now.keys[-1]:
				now = now.value[-1]
				continue
			for i in range(len(now.keys)):
				if key<now.keys[i]:
					now = now.value[i]
					continue
		
		for i in range(len(now.keys)):
			if now.keys[i]==key:
				return now.value[i]
				
		return default
	
	def all(self):
		now = self.root
		
		while not now.leaf:
			now = now.value[0]
		
		out = '→'.join([str(i) for i in now.value])
		while now.next:
			now = now.next
			out += '→'
			out += '→'.join([str(i) for i in now.value])
		
		return out


class Node:
	def __init__(self, order=3, leaf=True):
		self.order = order
		self.leaf = leaf
		self.mid = order//2
		self.mid_v = (order+1)//2
		self.keys = []
		self.value = []
		self.parent = None
		self.next = None
	
	def add_value(self,key,value):
		length = len(self.keys)
		if self.keys:
			if key>self.keys[-1]:
				self.keys.append(key)
				self.value.append(value)
			else:
				for i in range(length):
					if key<=self.keys[i]:
						self.keys.insert(i,key)
						self.value.insert(i,value)
						break
		else:
			self.keys.append(key)
			self.value.append(value)
		
		if length+1==self.order:
			self.split()
	
	def add_Node(self,key,node):
		length = len(self.keys)
		if key>self.keys[-1]:
			self.keys.append(key)
			self.value.append(node)
		else:
			for i in range(length):
				if key<self.keys[i]:
					self.keys.insert(i,key)
					self.value.insert(i+1,node)
					break
				elif key==self.keys[i]:
					self.value[i] = self.value[i+1]	
					self.value[i+1] = node

		if length+1==self.order:
			self.split()

	def split(self):
		if self.leaf:
			if self.parent == None:
				left = Node(self.order)
				right = Node(self.order)
				
				left.keys, right.keys = self.keys[:self.mid], self.keys[self.mid:]
				left.value, right.value = self.value[:self.mid], self.value[self.mid:]
				left.parent = right.parent = self
				
				left.next = right
				
				self.keys = [self.keys[self.mid]]
				self.value = [left, right]
				self.leaf = False
			else:
				right = Node(self.order)
				key = self.keys[self.mid]
				
				self.keys, right.keys = self.keys[:self.mid], self.keys[self.mid:]
				self.value, right.value = self.value[:self.mid], self.value[self.mid:]
				
				right.next = self.next
				self.next = right
				
				right.parent = self.parent
				self.parent.add_Node(key,right)
		else:
			if self.parent == None:
				left = Node(self.order, False)
				right = Node(self.order, False)
				
				left.keys, right.keys = self.keys[:self.mid], self.keys[self.mid+1:]
				left.value, right.value = self.value[:self.mid_v], self.value[self.mid_v:]
				left.parent = right.parent = self
				
				for i in left.value:
					i.parent = left
				for i in right.value:
					i.parent = right
				
				self.keys = [self.keys[self.mid]]
				self.value = [left,right]
			else:
				right = Node(self.order, False)
				
				key = self.keys[self.mid]
				
				right.keys,right.value = self.keys[self.mid+1:],self.value[self.mid_v:]
				self.keys,self.value = self.keys[:self.mid],self.value[:self.mid_v]
				
				right.parent = self.parent
				for i in right.value:
					i.parent = right
				
				self.parent.add_Node(key,right)
	
	
	def delete_value(self, key):
		if self.leaf:
			length = len(self.keys)
			index = self.keys.index(key)
				
			for i in range(index,len(self.value)-1):
				self.value[i] = self.value[i+1]
			
			self.value[-1] = None
			self.value.remove(None)
			self.keys.remove(key)
			
			parent = self.parent
			if len(self.value)==0 and self.parent:
				now_index = parent.value.index(self)
				if now_index:
					parent.delete_nodex(self, now_index)
				else:
					change = now.next.keys[0]
					parent.change_key(key, change)
			elif now.parent:
				change = now.value[0]
				parent.change_key(key, change)
	
	def delete_node(self, node, index):
		if not self.leaf:
			length = len(self.keys)
			if node in self.value:
				index = self.value.index(node)
			else:
				return 
			
			if index:
				self.value[index-1].next = self.value[index].next
			elif node.leaf:
				self_index = 0
				now = self
				while now.parent:
					self_index = now.parent.value.index(now)
					if self_index:
						break
					now = now.parent
				if self_index:
					now = now.value[self_index-1]
					while not now.leaf:
						now = now.value[-1]
					
					now.next = self.value[index].next
			
			self.keys.remove(self.keys[max(0,index-1)])
			self.value.remove(node)
			
			if len(self.value)==1:
				self.Merge(key,change)
				if self.parent != None:
					self.parent.change_key(key, change)
					
	def change_key(self, key, change):
		if key in self.keys:
			self.keys[self.keys.index(key)] = change
			if self.parent:
				self.parent.change_key(key, change)
					
	def Merge(self,key,change):
		global BPT
		if self.parent == None:
			child = self.value[0]
			
			self.value = child.value
			self.keys = child.keys
			
			if type(self.value[0]) != type(Node()):
				self.leaf = True
			else:
				for i in self.value:
					i.parent = self
		else:
			old = self.value[0]
			self_index = self.parent.value.index(self)
			
			if self_index!=0:
				beside = self.parent.value[self_index-1]
				old.parent = beside
				beside.value[-1].next = old
				beside.value.append(old)
				beside.keys.append(old.keys[0])
			else:
				beside = self.parent.value[self_index+1]
				old.parent = beside
				old.next = beside.value[0]
				beside.value.insert(0,old)
				beside.keys.insert(0,old.next.keys[0])
				
			if len(beside.keys)>=self.order:
				print(BPT)
				beside.split()

			self.parent.delete_node(self, key, change)
		
	


if __name__=='__main__':
	data = [i for i in range(30)]
	shuffle(data)
	test = [i for i in range(30)]

	BPT = BpTree(3)
	for i in data: 
		BPT[i] = i
	
	for i in test:
		print(f'delete {i}')
		BPT.delete(i)
		print(BPT)
		print(BPT.all())
	sys.exit()
	
	'''
	data = 10000
	BPT = BpTree(12)
	test = []
	
	L_total = 0
	B_total = 0
	print('{} datas test'.format(data))
	print('=======================')
	data = [i for i in range(data)]
	shuffle(data)

	t0 = time()
	for i in data: test.append(i)
	t1 = time()
	for i in data: BPT[i]=i
	t2 = time()
	
	print('List append   : {:>5}ms'.format(round((t1-t0)*1000)))
	print('BpTree append : {:>5}ms'.format(round((t2-t1)*1000)))
	print('=======================')

	t0 = time()
	-1 in test
	t1 = time()
	-1 in BPT
	t2 = time()
	
	print('List search   : {:>5}us'.format(round((t1-t0)*1000000)))
	print('BpTree search : {:>5}us'.format(round((t2-t1)*1000000)))
	print('=======================')
	shuffle(data)
	
	t0 = time()
	for i in data: test.remove(i)
	t1 = time()
	for i in data: 
		try:
			BPT.delete(i)
		except:
			print(BPT)
			print(BPT.all())
			sys.exit()
	t2 = time()
	
	print('List delete   : {:>5}us'.format(round((t1-t0)*1000000)))
	print('BpTree delete : {:>5}us'.format(round((t2-t1)*1000000)))
	print('=======================')
	'''
