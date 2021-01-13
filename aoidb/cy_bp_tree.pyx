import sys
import cython
sys.setrecursionlimit(2147483647)

cdef class Node:
  cdef public int order
  cdef public int mid
  cdef public int mid_v
  cdef public list keys
  cdef public list value
  cdef public int leaf
  cdef public Node parent
  cdef public Node next

  """
  B+Tree node object
  """
  def __cinit__(self, int order=3, leaf=True):
    self.order = order
    self.leaf = leaf
    self.mid = order//2
    self.mid_v = self.mid+1
    self.keys = []
    self.value = []
    self.parent = self.next = None
  
  cpdef add_value(self, new_key, new_value):
    '''
    新增資料至節點
    '''
    cdef int length = len(self.keys)
    cdef int i
    
    for i in range(length):
      if new_key<=self.keys[i]:
        self.keys.insert(i, new_key)
        self.value.insert(i, new_value)
        break
    else:
      self.keys.append(new_key)
      self.value.append(new_value)
    
    #如果長度達到上限就分裂
    if length+1 == self.order:
      self.split()
  
  cdef add_Node(self, key, Node node):
    '''
    以節點為資料新增至節點
    '''
    cdef length = len(self.keys)
    cdef int i

    #大於往右放
    for i in range(length):
      if key<self.keys[i]:
        self.keys.insert(i,key)
        self.value.insert(i+1,node)
        break
      elif key==self.keys[i]:
        self.value[i] = self.value[i+1]  
        self.value[i+1] = node
    else:
      self.keys.append(key)
      self.value.append(node)
    
    #如果長度達到上限就分裂
    if length+1==self.order:
      self.split()

  cpdef split(self):
    '''
    節點分裂生長
    '''
    
    #如果分裂的節點是葉節點
    #分裂後產生a,b節點 以b節點的最小值為新key(用來給parent新增value)
    #如果沒有parent節點就以自己為parent，key為新key，value就是a,b節點

    cdef Node left, right, child
    cdef int mid=self.mid, mid_v=self.mid_v, order=self.order
    cdef list keys=self.keys, value=self.value
    if self.leaf:
      if self.parent is None:
        # 自己為root節點: 保持自己為root節點
        # before                     after
        #                             [2] ← self
        # [1 2 3] ← self    left → [1]   [2 3] ← right
        
        left = Node(order)
        right = Node(order)
        
        left.keys, right.keys = keys[:mid], keys[mid:]
        left.value, right.value = value[:mid], value[mid:]
        left.parent = right.parent = self
        
        left.next = right               #建立linked關係
        
        self.keys = [keys[mid]]         #新key
        self.value = [left, right]      #沒有parent所以自己為root 將分裂出的節點往下放
        self.leaf = False               #解除leaf資格
      
      else:
        # 有parent則要把新產生的節點做add_node
        #   before                   after
        #    [2]                     [2 3] ← add_node(3, right)
        # [1]   [2 3 4] ← self    [1] [2] [3 4] ← right
        #                          self↑

        right = Node(order)      #因為有parent 所以分裂出的左半就給自己
        new_key = keys[mid]    
        
        self.keys, right.keys = keys[:mid], keys[mid:]
        self.value, right.value = value[:mid], value[mid:]
        
        #before：self->next after:self->right->next
        right.next = self.next
        self.next = right  
        
        right.parent = self.parent      #設定新節點的parent
        self.parent.add_Node(new_key,right)    #right加入parent的value
    
    else:
      #分裂的節點不是leaf的情況
      if self.parent == None:
        #如果沒有parent 就以中央的值為新的母節點的key 
        #  before                          after
        #                                    [2]  <- self
        #    [1  2  3]  <- self         [1]        [3]
        # [0] [1] [2] [3]            [0]  [1]   [2]   [3]
        
        left = Node(order, False)
        right = Node(order, False)
        
        left.keys, right.keys = keys[:mid], keys[mid+1:]
        left.value, right.value = value[:mid_v], value[mid_v:]
        left.parent = right.parent = self
        
        #分裂後的節點要重新設定parent關係
        for child in left.value:
          child.parent = left
        for child in right.value:
          child.parent = right
        
        self.keys = [keys[mid]]
        self.value = [left,right]
      
      else:
        right = Node(order, False)
        new_key = keys[mid]
        
        self.keys, right.keys = keys[:mid], keys[mid+1:]
        self.value, right.value = value[:mid_v], value[mid_v:]
        
        #分裂後的節點要重新設定parent關係
        right.parent = self.parent
        for child in right.value:
          child.parent = right
        
        self.parent.add_Node(new_key, right)
  
  cpdef delete_value(self,del_key):
    '''
    刪除指定資料
    '''
    cdef int length = len(self.keys)-1
    cdef int del_index,i

    #刪除的同時找用來替換的值
    #替換的值: 刪除的值右側一格的值
    for i in range(length):
      if self.keys[i] == del_key:
        #要刪除的值
        #要刪除的值的索引
        del_value = self.value[i]
        del_index = i

        #下一格的值(替換值)
        replace_value = self.keys[i+1]
        break
    else:
      #目標不存在
      if del_key!=self.keys[-1]:
        return
      
      del_value = self.value[-1]
      del_index = length

      #用來替換的值
      #如果右邊已經沒有值 代表不需要替換(如果有節點的key是這個key 那個key一定會被刪掉)
      if self.next is not None:
        replace_value = self.next.keys[0]
      else:
        replace_value = None
    
    #刪除目標key及對應的值
    del self.value[del_index]
    del self.keys[del_index]
    
    #如果有父節點 呼叫其進行替換
    if self.parent is not None:
      self.parent.replace(del_value, replace_value)
    
    #如果本身已空
    #呼叫父節點進行刪除空節點動作
    #同時重建link
    cdef int index
    cdef Node before, after, target
    if len(self.keys)==0:
      if self.parent is not None:
        #重建link
        #持續往上找直到左側有節點 並找到該節點最右側的值
        #如果本身為最左側節點則不進行relink
        before = self
        now = self.parent

        while now is not None:
          index = now.value.index(before)

          #左側有節點
          if index:
            #尋找左側節點的最右側葉節點
            target = now.value[index-1]
            while not target.leaf:
              target = target.value[-1]
            
            #relink
            target.next = self.next
            break

          #如果左側沒有節點 繼續往上找
          before = now
          now = now.parent
        
        #最後呼叫父節點刪除自己
        self.parent.delete_empty_node(self)
  
  cdef replace(self, a, b):
    '''
    替換指定鍵值
    '''
    cdef int i
    for i in range(len(self.keys)):
      if self.keys[i]==a:
        self.keys[i]=b

    if self.parent is not None:
      self.parent.replace(a, b)
  
  cdef delete_empty_node(self,Node node):
    '''
    刪除空的節點
    '''
    cdef int length = len(self.keys)
    cdef int i
    
    #節點連帶左上的key值一起刪
    #因此最左側的節點獨立檢察
    if self.value[0] is node:
      del self.value[0]
      del self.keys[0]
    else:
      for i in range(length):
        if self.value[i+1] is node:
          del self.value[i+1]
          del self.keys[i]
          break
    
    #當自己只剩下一個子節點時 就跟隔壁的節點合併並呼叫父節點刪除自己
    cdef Node last_node, new_parent, now
    if len(self.keys)==0 and self.parent is not None:
      #剩下的最後一個節點
      last_node = self.value[0]
      for i in range(len(self.parent.keys)):
        #找同層級左側的節點 來把剩下的那個節點add_node進去
        if self.parent.value[i+1] is self:
          #給剩下那個節點的新parent
          new_parent = self.parent.value[i]
          last_node.parent = new_parent

          #找剩餘節點最小的key值來作為新的key值(添加進去的節點在最右側)
          now = last_node
          while not now.leaf:
            now = now.value[0]

          new_parent.keys.append(now.keys[0])
          new_parent.value.append(last_node)
          break
      else:
        #在最左側 找右側一個的節點來add
        new_parent = self.parent.value[1]
        last_node.parent = new_parent

        #找新的母節點最小的key值來作為新的key值(添加進去的節點在最左側)
        now = new_parent
        while not now.leaf:
          now = now.value[0]
        
        new_parent.keys.insert(0, now.keys[0])
        new_parent.value.insert(0, last_node)

      #如果超量就split
      if len(new_parent.keys)==new_parent.order:
        new_parent.split()
      #因為做完此操作後自己就會變空 因此呼叫父節點刪除自己
      self.parent.delete_empty_node(self)


cdef class BpTree:
  cdef public int order
  cdef public Node root

  """
  B+Tree object
  """
  def __cinit__(self, int order=3):
    self.order = order
    self.root = Node(order)

  def __str__(self):
    cdef Node root = self.root, now
    cdef int b_layer = 1, layer
    cdef list queue

    out = '|{}|'.format(','.join([str(i) for i in root.keys]))
    
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
    找到最小值之後往後迭代
    '''
    cdef int i
    cdef Node now = self.root
    
    #往左找最小
    while not now.leaf:
      now = now.value[0]
    
    #開始迭代，利用yield產生genertor
    while now:
      for i in range(len(now.keys)):
        yield now.keys[i]
      now = now.next
  
  def __getitem__(self, key):
    cdef int i
    cdef Node now = self.root
    
    #大於等於找右邊
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
    '''
    如果現有的節點已經存有欲修改的key則直接修改
    如果沒有就新增資料(add_value)
    '''
    cdef int i
    cdef Node now = self.root
    
    #大於等於找右邊
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
        break
    else:
      now.add_value(key, value)
  
  cpdef delete(self, key):
    cdef int i
    cdef Node now = self.root
    
    #大於等於找右邊
    while not now.leaf:
      if key >= now.keys[-1]:
        now = now.value[-1]
      else:
        for i in range(len(now.keys)):
          if key<now.keys[i]:
            now = now.value[i]
            break

    now.delete_value(key)

    if self.root.keys==[]:
      if self.root.value==[]:
        self.root = Node(self.order)
      else:
        self.root = self.root.value[0]
        self.root.parent = None
  
  def __contains__(self,key):
    return self.__getitem__(key) is not None
  
  def __len__(self):
    cdef int i,res

    res = 0
    for i in self.__iter__():
      res += 1
    
    return res
  
  def size(self):
    '''
    求節點數量，直接使用BFS遍歷
    '''
    cdef int res = 0
    cdef list queue = [self.root]
    cdef Node now
    
    while queue:
      now = queue.pop()
      if now.leaf:
        res += len(now.value)
      else:
        queue += now.value
        res += len(now.value)
    
    return res
    
  def items(self):
    '''
    從最小值往後找，使用yield產生generator
    '''
    cdef int i
    cdef Node now = self.root
    
    while not now.leaf:
      now = now.value[0]
      
    while now:
      for i in range(len(now.value)):
        yield (now.keys[i], now.value[i])
      now = now.next
  
  def keys(self):
    '''
    從最小值往後找，使用yield產生generator
    '''
    cdef Node now = self.root
    
    while not now.leaf:
      now = now.value[0]
      
    while now:
      for i in now.keys:
        yield i
      now = now.next
    
  def values(self):
    '''
    從最小值往後找，使用yield產生generator
    '''
    cdef Node now = self.root
    
    while not now.leaf:
      now = now.value[0]
      
    while now:
      for i in now.value:
        yield i
      now = now.next

  
  def get(self,key,default=0):
    '''
    getitem有預設值的版本
    '''
    get = self.__getitem__(key)
    return default if get==None else get

    
cdef class BpTreeSet:
  cdef public int order
  cdef public Node root

  """
  B+Tree object
  """
  def __cinit__(self, int order):
    self.order = order
    self.root = Node(order)
  
  def __str__(self):
    cdef Node root = self.root, now
    cdef int b_layer = 1, layer
    cdef list queue

    out = '|{}|'.format(','.join([str(i) for i in root.keys]))
    
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
    找到最小值之後往後迭代
    '''
    cdef int i
    cdef Node now = self.root
    
    #往左找最小
    while not now.leaf:
      now = now.value[0]
    
    #開始迭代，利用yield產生genertor
    while now:
      for i in range(len(now.keys)):
        yield now.keys[i]
      now = now.next
  
  def add(self, key):
    '''
    新增資料(add_value)
    '''
    cdef int i
    cdef Node now = self.root
    
    #大於等於找右邊
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
        now.value[i]=key
        break
    else:
      now.add_value(key, key)
  append = add

  cpdef delete(self, key):
    cdef int i
    cdef Node now = self.root
    
    #大於等於找右邊
    while not now.leaf:
      if key >= now.keys[-1]:
        now = now.value[-1]
      else:
        for i in range(len(now.keys)):
          if key<now.keys[i]:
            now = now.value[i]
            break

    now.delete_value(key)

    if self.root.keys==[]:
      if self.root.value==[]:
        self.root = Node(self.order)
      else:
        self.root = self.root.value[0]
        self.root.parent = None
  remove = delete

  def __contains__(self,key):
    return self.__getitem__(key) is not None
  
  def __len__(self):
    cdef int i,res

    res = 0
    for i in self.__iter__():
      res += 1
    
    return res
  
  def size(self):
    '''
    求節點數量，直接使用BFS遍歷
    '''
    cdef int res = 0
    cdef list queue = [self.root]
    cdef Node now
    
    while queue:
      now = queue.pop()
      if now.leaf:
        res += len(now.value)
      else:
        queue += now.value
        res += len(now.value)
    
    return res
    
  def get(self,key,int default=0):
    '''
    getitem有預設值的版本
    '''
    get = self.__getitem__(key)
    return default if get==None else get