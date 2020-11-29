class BpTree:
	'''
	B+Tree Object
	Can be used as a dict
	'''
	class Node:
		"""
		B+Tree node object
		"""
		def __init__(self, order=3, leaf=True):
			self.order = order
			self.leaf = leaf
			
			self.mid = order//2
			self.mid_v = self.mid+1
			self.keys = []
			self.value = []

			self.parent = None
			self.next = None
		
		def add_value(self,key,value):
			'''
			add new data to this node
			'''
			length = len(self.keys)
			
			if not self.keys or key>self.keys[-1]:
				self.keys.append(key)
				self.value.append(value)
			else:
				for i in range(length):
					if key<=self.keys[i]:
						self.keys.insert(i,key)
						self.value.insert(i,value)
						break
			
			#split if meet the length limit
			if length+1==self.order:
				self.split()
		
		def add_Node(self,key,node):
			'''
			add new child node to this node
			'''
			length = len(self.keys)
			
			#if key is biggest, put it to the right side
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
			
			#split if meet the length limit
			if length+1==self.order:
				self.split()

		def split(self):
			'''
			split and grow
			'''
			
			#如果分裂的節點是葉節點
			#分裂後產生a,b節點 以b節點的最小值為新key(用來給parent新增value)
			#如果沒有parent節點就以自己為parent，key為新key，value就是a,b節點
			if self.leaf:
				if self.parent == None:
					# before             after
					#                      2    <- self
					# 1 2 3  <- self     1   2 3
					
					left = BpTree.Node(self.order)
					right = BpTree.Node(self.order)
					
					left.keys, right.keys = self.keys[:self.mid], self.keys[self.mid:]
					left.value, right.value = self.value[:self.mid], self.value[self.mid:]
					left.parent = right.parent = self
					
					left.next = right					#建立linked關係
					
					self.keys = [self.keys[self.mid]]	#新key
					self.value = [left, right]			#沒有parent所以自己為root 將分裂出的節點往下放
					self.leaf = False					#解除leaf資格
				
				else:
					#沒有parent則要把新產生的節點做add_node
					# before            after
					#                     2   <- add_node
					# 1 2 3  <- self -> 1   2 3 <- right
					
					right = BpTree.Node(self.order)			#因為有parent 所以分裂出的左半就給自己
					key = self.keys[self.mid]		
					
					self.keys, right.keys = self.keys[:self.mid], self.keys[self.mid:]
					self.value, right.value = self.value[:self.mid], self.value[self.mid:]
					
					right.next = self.next				#before：self->next after:self->right->next
					self.next = right					#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
					
					right.parent = self.parent			#設定新節點的parent
					self.parent.add_Node(key,right)		#right加入parent的value
			
			else:
				#分裂的節點不是leaf的情況
				if self.parent == None:
					#如果沒有parent 就以中央的值為新的母節點的key 
					#  before                    after
					#                              2   <- self
					#  1 2 3  <- self    ->     1     3
					# 0 1 2 3            ->   0  1  2   3
					
					left = BpTree.Node(self.order, False)
					right = BpTree.Node(self.order, False)
					
					left.keys, right.keys = self.keys[:self.mid], self.keys[self.mid+1:]
					left.value, right.value = self.value[:self.mid_v], self.value[self.mid_v:]
					left.parent = right.parent = self
					
					#分裂後的節點要重新設定parent關係
					for i in left.value:
						i.parent = left
					for i in right.value:
						i.parent = right
					
					self.keys = [self.keys[self.mid]]
					self.value = [left,right]
				
				else:
					#有parent的話也是一樣的分裂方法
					#只是往上產生的新節點要給母節點做add_node
					#          before                       after
					#                        add_node ->      2
					# self ->  1 2 3    ->       self ->   1      3
					#         0 1 2 3   ->               0   1  2   3
					
					right = BpTree.Node(self.order, False)
					
					key = self.keys[self.mid]
					
					right.keys,right.value = self.keys[self.mid+1:],self.value[self.mid_v:]
					self.keys,self.value = self.keys[:self.mid],self.value[:self.mid_v]
					
					#分裂後的節點要重新設定parent關係
					right.parent = self.parent
					for i in right.value:
						i.parent = right
					
					self.parent.add_Node(key,right)


	def __init__(self,order):
		self.order = order
		self.root = self.Node(order)
	
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
		'''
		go to the node at the left side's end
		and iterate it to the right side
		'''
		now = self.root
		
		while not now.leaf:
			now = now.value[0]
		
		#make a generator
		while now:
			for i in range(len(now.key)):
				yield now.key[i]
			now = now.next
	
	def __getitem__(self,key,p=False):
		now = self.root
		
		#if bigger to the right side
		while not now.leaf:
			if key>=now.keys[-1]:
				now = now.value[-1]
			else:
				for i in range(len(now.keys)):
					if key<now.keys[i]:
						now = now.value[i]
						break
		
		#find the data in the target node
		for i in range(len(now.keys)):
			if now.keys[i]==key:
				return now.value[i]
				
		return None
	
	def __setitem__(self, key, value):
		'''
		already has: change it
		or: add node
		'''
		now = self.root
		
		while not now.leaf:
			if key >= now.keys[-1]:
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
		return self.__getitem__(key) is not None
	
	def __len__(self):
		res = 0
		for i in self.__iter__():
			res += 1
		
		return res
	
	def size(self):
		'''
		caculate we have how many nodes
		use BFS to search that
		'''
		res = 0
		queue = [self.root]
		
		while queue:
			now = queue.pop()
			res += len(now.keys)
			if not now.leaf:
				queue += now.value
		
		return res
		
	def items(self):
		'''
		從最小值往後找，使用yield產生generator
		'''
		now = self.root
		
		while not now.leaf:
			now = now.value[0]
			
		while now:
			for i in range(len(now.value)):
				yield (now.keys[i], now.value[i])
			now = now.next
	
	
	def get(self,key,default=0):
		'''
		getitem有預設值的版本
		'''
		get = self.__getitem__(key)
		return default if get==None else get
	
	def all(self):
		now = self.root
		
		while not now.leaf:
			now = now.value[0]
		
		allvalue = [str(i) for i in now.value]
		while now.next:
			now = now.next
			allvalue += [str(i) for i in now.value]
		
		return '→'.join(allvalue)
