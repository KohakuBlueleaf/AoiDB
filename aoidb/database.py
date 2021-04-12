import os,sys
import pickle
from time import time
from copy import copy,deepcopy
from ._bp_tree import BpTree as bpt
from .cy_bp_tree import BpTree
from ._functions import *


sys.setrecursionlimit(10000000)


class AoiDB:
  class Data:
    """
    DB中的Data物件，儲存資料的最小單位
    """
    def __init__(self, id:int):
      self.id = id
      self.data = {}  #儲存資料
      
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
      self.data = node.data

    def __len__(self):
      return len(self.data)

    def __iter__(self):
      for i in self.data:
        yield i
    
    def __hash__(self):
      return hash(f'AoiDB_Data_{self.id}')

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
      self.data[key] = value
    
  
  class DataSet:
    """
    DataSet物件 當db須回傳多筆資料時使用此物件儲存
    """
    def __init__(self, data):
      self.data = list(data)
    
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
    
    self.__column = []
    self.type = {}
    self.__index = {}
    self.cmp = {}
    self.command_list = []
    
    self.__id_list = BpTree(16)
    self.__idmax = 0
  
  def __iter__(self):
    for i in self.all_data:
      yield i
  
  def show(self, sort_by=''):
    out = f'AoiDB_{self.name }:\n'
    length = len(self.__column)
    spliter = '\n├──────────'+'┼──────────'*length+'┤\n│'
    out += '┌──────────'+'┬──────────'*length+'┐\n│'
    
    out += '{:>10}│'.format('id')
    
    for i in self.__column:
      out += '{}│'.format(get_str(i))
    
    if sort_by:
      if sort_by in self.__index:
        res = sum([i for i in self.__index[sort_by]],[])
      else:
        if sort_by in self.cmp:
          key = lambda i:self.cmp[sort_by](i[sort_by])
        else:
          key = lambda i:i[sort_by]
        res = sorted(self.all_data, key=key)
        
      for i in res:
        out += spliter
        out += '{}│'.format(get_str(i))
        for j in self.__column:
          out += '{}│'.format(get_str(self.__id_list[i][j]))
    else:
      for i in self.all_data:
        out += spliter
        out += '{}│'.format(get_str(i.id))
        for j in self.__column:
          out += '{}│'.format(get_str(i[j]))
    
    out += '\n└──────────'+'┴──────────'*length+'┘'
    
    return out
  
  __str__ = show
  
  def __getitem__(self, key):
    if key in self.__id_list:
      return self.__id_list[key]
  
  def __contains__(self, id):
    return id in self.__id_list

  def save(self, path=''):
    if path=='':
      if self.path=='':
        path = self.name
      else:
        path = self.path

    if path[-4:]!='.aoi':
      path+='.aoi'
      
    self.path = path
    all_data = (
      self.name, 
      self.type,
      self.all_data,
      self.__column, 
      [i for i in self.__index.keys()],
      self.__idmax
    )
    with open(path if path!='.aoi' else self.path, 'wb') as f:
      pickle.dump(all_data, f)
  
  def load(self, path=''):
    if not self.path:
      self.path = path
    if not path:
      path = self.path
    
    with open(path, 'rb') as f:
      all_data = pickle.load(f)
    
    self.name, self.type, self.all_data, self.__column, self.__index, self.__idmax = all_data
    self.__id_list = BpTree(16)

    for data in self.all_data:
      self.__id_list[data.id] = data
      #to fit new type of Data
      now = data
      if hasattr(now, 'item'):
        new = AoiDB.Data(now.id)
        new.data = now.data
      elif hash(now)!=hash(f'AoiDB_Data_{now.id}'):
        new = self.Data(now.id)
        new.data = now.data
      
    #to fit new type of index
    if type(self.__index)==dict:
      for col, bptree in self.__index.items():
        if not hasattr(bptree, 'delete'):
          new_tree = BpTree(16)
          for key, value in bptree.items():
            new_tree[key] = value if type(value)==int else value.id
          self.__index[col] = new_tree
    else:
      index_col = self.__index
      self.__index = {}
      for col in index_col:
        new_index = BpTree(16)
        for i in self.all_data:
          new_index[i[col]] = new_index.get(i[col], []).append(i.id)
        self.__index[col] = new_index

  def get_by_id(self, id):
    return self.__id_list[id]
  
  def get_e(self, **kwargs):
    final = None
    for key,target in kwargs.items():
      if key in self.__index:
        now = set(self.__id_list[i] for i in self.__index[key].get(target,[]))
      else:
        now = set([i for i in self.all_data if i[key]==target])
      if final is None:
        final = now
      else:
        final &= now
    
    return self.__dataset(final)
  
  def get_l(self, **kwargs):
    final = None
    for key,target in kwargs.items():
      now = set([i for i in self.all_data if i[key]<target])
      if not final:
        final = now
      else:
        final &= now
    return self.__dataset(final)
  
  def get_g(self, **kwargs):
    final = None
    for key,target in kwargs.items():
      now = set([i for i in self.all_data if i[key]>target])
      if not final:
        final = now
      else:
        final &= now
    return self.__dataset(final)
  
  def get(self, **kwargs):
    if 'mode' in kwargs:
      mode = kwargs['mode']
      del kwargs['mode']
    else:
      mode = '='
    
    if 'id' in kwargs:
      return self.get_by_id(kwargs['id'])
    elif mode=='=':
      return self.get_e(**kwargs)
    elif mode=='<':
      return self.get_l(**kwargs)
    elif mode=='>':
      return self.get_g(**kwargs)
    else:
      raise AttributeError('\'mode\' should be "=", "<", or ">"')
      

  def add_data(self, **kwargs):
    new = self.Data(self.__idmax)
    
    self.all_data.append(new)
    self.__id_list[self.__idmax] = new
    self.__idmax += 1
    
    for key,value in kwargs.items():
      new.data[key] = value
      
    for i in self.__column:
      if i not in new.data:
        new.data[i] = self.type[i]()
        
      if i in self.__index:
        if new.data[i] in self.__index[i]:
          self.__index[i][new.data[i]].append(new.id)
        else:
          self.__index[i][new.data[i]] = [new.id]
    return new
  
  def delete(self, node:Data):
    node = self.__id_list[node.id]
    self.all_data.remove(node)
    
    for key,tree in self.__index.items():
      tree[node[key]].remove(node.id)
      if tree[node[key]] == []:
        tree.delete(node[key])
    
    self.__id_list.delete(node.id)
  
  def delete_by_value(self, key:str, value, mode='='):
    res = self.get(key, value, mode)
    for i in res:
      self.delete(i)
  
  def change_value(self, id, **kwargs):
    target = self.__id_list[id]
    for key,value in kwargs.items():
      if key in self.__index:
        self.__index[key][target[key]].remove(target.id)
      target[key] = value

      if key in self.__index:
        if value in self.__index[key]:
          self.__index[key][value].append(target.id)
        else:
          self.__index[key][value] = [target.id]
    self.command_list.append([id,kwargs])

  def col(self):
    return list(self.type.keys())

  def add_col(self, col, data_type):
    if col not in self.type:
      self.type[col] = type(data_type)
      self.__column.append(col)
      
      for i in self.all_data:
        i.data[col] = type(data_type)()
  
  def del_col(self, col):
    del self.type[col]
    self.__column.remove(col)
    
    for i in self.all_data:
      del i.data[col]
    
    if col in self.__index:
      del self.__index[col]
  

  def create_index(self, key:str):
    self.__index[key] = BpTree(16)
    self.__index[key][self.type[key]()] = []
    index = self.__index[key]
    
    for i in self.all_data:
      value = i[key]
      if value in index:
        index[value].append(i.id)
      else:
        index[value] = [i.id]
  
  def delete_index(self, key:str):
    del self.__index[key]
  
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


class AoiDB2:
  class Data:
    """
    DB中的Data物件，儲存資料的最小單位
    """
    def __init__(self, id:int, data):
      self.id = id
      self.data = data  #儲存資料
      
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
      self.data = node.data

    def __len__(self):
      return len(self.data)

    def __iter__(self):
      for i in self.data:
        yield i
    
    def __hash__(self):
      return hash(f'AoiDB_Data_{self.id}')

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
      self.data[key] = value
  
  class DataSet:
    """
    DataSet物件 當db須回傳多筆資料時使用此物件儲存
    """
    def __init__(self, data):
      self.data = list(data)
    
    def __str__(self):
      out = 'DataSet('
      for i in self.data:
        out += str(i)
        out += ','
      
      return out[:-1] + ')' 

    __repr__ = __str__

    def __getitem__(self,key):
      return self.data[key]
    
    def __iter__(self):
      for i in self.data:
        yield i
    
    def __len__(self):
      return len(self.data)
    
    def __add__(self, other):
      return AoiDB2.DataSet(self.data + other.data)


  def __init__(self, name=''):
    self.name = name
    self.path = ''
    
    self.__column = []
    self.__types = {}
    self.__index = {}
    self.__datas = {}
    self.__multi = {}
    
    self.__id_list = BpTree(16)
    self.__idmax = 0
  
  def __iter__(self):
    for i in self.__id_list.values():
      yield AoiDB2.Data(id, {key: self.__datas[key][i] for key in self.__column})
  
  def show(self):
    out = f'AoiDB_{self.name }:\n'
    length = len(self.__column)
    spliter = '\n├──────────'+'┼──────────'*length+'┤\n│'
    out += '┌──────────'+'┬──────────'*length+'┐\n│'
    
    out += '{:>10}│'.format('id')
    
    for i in self.__column:
      out += '{}│'.format(get_str(i))
    
    for i in self.__id_list:
      out += spliter
      out += '{}│'.format(get_str(i))
      for j in self.__datas.values():
        out += '{}│'.format(get_str(j[self.__id_list[i]]))
    
    out += '\n└──────────'+'┴──────────'*length+'┘'
    
    return out
  
  __str__ = __repr__ = show
  
  def __getitem__(self, key):
    if key in self.__id_list:
      return self.__id_list[key]
  
  def __contains__(self, id):
    return id in self.__id_list

  def save(self, path=''):
    if path=='':
      if self.path=='':
        path = self.name
      else:
        path = self.path

    if path[-4:] != 'aoi2':
      path += 'aoi2'
    
    id_list = [i for i in self.__id_list]
    datas = [
      id_list,
      self.__column, 
      self.__types, 
      self.__multi,
      [i for i in self.__index.keys()],
      self.__datas, 
      self.__idmax
    ]
    with open(path, 'wb') as f:
      pickle.dump(datas, f)
  
  def load(self, path):
    if not self.path:
      self.path = path
    if not path:
      path = self.path
      
    with open(path, 'rb') as f:
      datas = pickle.load(f)
    
    id_list, self.__column, self.__types, self.__multi, index_col, self.__datas, self.__idmax = datas
    self.__id_list = BpTree(16)
    for i in range(len(id_list)):
      self.__id_list[id_list[i]] = i

    for i in index_col:
      if self.__multi[i]:
        self.__index[i] = BpTree(16)
        for id, index in self.__id_list.items():
          data = self.__datas[i][index]
          for value in data:
            if value in self.__index[i]:
              self.__index[i][value].append(id)
            else:
              self.__index[i][value] = [id]
      else:
        self.create_index(i)
    self.path = path

  def get_by_id(self, id):
    index = self.__id_list[id]
    if index is None:
      raise ValueError('object is not exist')
    return AoiDB2.Data(id, {key: self.__datas[key][index] for key in self.__column})
  
  def get_e(self, **kwargs):
    final = None
    for key,value in kwargs.items():
      if key in self.__index:
        if self.__multi[key] and type(value)==list:
          data = set(self.__index[key][value[0]])
          for v in value[1:]:
            data &= set(self.__index[key][v])
            
        else:
          if value in self.__index[key]:
            data = set(self.__index[key][value])
          else:
            data = set()
      else:
        data = self.__datas[key]
        data = set(i for i,j in self.__id_list.items() if data[j]==value)

      if final is None:
        final = data
      else:
        final &= data

    return AoiDB2.DataSet(self.get_by_id(i) for i in final)

  def get_l(self, **kwargs):
    final = None
    for key,value in kwargs.items():
      if self.__multi[key]:
        raise TypeError("A multi-value column cannot use '<' '>' search mode.")
      if key in self.__index:
        data = set()
        for i,v in self.__index[key].items():
          if i>=value:
            break
          data |= set(v)
      else:
        data = self.__datas[key]
        data = set(i for i,j in self.__id_list.items() if data[j]<value)

      if final is None:
        final = data
      else:
        final &= data

    if final is None:
      final = set()
    return AoiDB2.DataSet(self.get_by_id(i) for i in final)
  
  def get_g(self, **kwargs):
    final = None
    for key,value in kwargs.items():
      if self.__multi[key]:
        raise TypeError("A multi-value column cannot use '<' '>' search mode.")
      data = self.__datas[key]
      data = set(i for i,j in self.__id_list.items() if data[j]>value)

      if final is None:
        final = data
      else:
        final &= data
        
    if final is None:
      final = set()
    return AoiDB2.DataSet(self.get_by_id(i) for i in final)
  
  def get(self, **kwargs):
    if 'mode' in kwargs:
      mode = kwargs['mode']
      del kwargs['mode']
    else:
      mode = '='
    
    #print(kwargs)
    if 'id' in kwargs:
    #  print(1)
      return self.get_by_id(kwargs['id'])
    elif mode=='=':
    #  print(2)
      return self.get_e(**kwargs)
    elif mode=='<':
    #  print(3)
      return self.get_l(**kwargs)
    elif mode=='>':
    #  print(4)
      return self.get_g(**kwargs)
    else:
      raise AttributeError('\'mode\' should be "=", "<", or ">"')
      

  def add_data(self, **kwargs):
    new_id = self.__idmax+1
    self.__idmax += 1
    self.__id_list[new_id] = len(self.__datas[self.__column[0]])
    for name, dtype in self.__types.items():
      if self.__multi[name]:
        if name in kwargs:
          self.__datas[name].append(kwargs[name])
        else:
          self.__datas[name].append([dtype()])

        if name in kwargs:
          for i in kwargs[name]:
            if type(i) != self.__types[name]:
              raise TypeError(f'The type of {name} should be {self.__types[name]}')
            else:
              if i in self.__index[name]:
                self.__index[name][i].append(new_id)
              else:
                self.__index[name][i] = [new_id]
        else:
          self.__index[name][dtype()].append(new_id)

      else:
        if name in kwargs:
          if type(kwargs[name]) != self.__types[name]:
            raise TypeError(f'The type of {name} should be {self.__types[name]}')
          self.__datas[name].append(kwargs[name])
        else:
          self.__datas[name].append(dtype())
        
        if name in self.__index:
          if self.__datas[name][-1] in self.__index[name]:
            self.__index[name][self.__datas[name][-1]].append(new_id)
          else:
            self.__index[name][self.__datas[name][-1]] = [new_id]

    return self.get_by_id(new_id)
  
  def delete(self, id):
    if id not in self.__id_list:
      raise ValueError("This id is not in this database.")
    for i in self.__id_list:
      if i>=id:
        self.__id_list[i] = self.__id_list[i]-1
    
    index = self.__id_list[id]
    self.__id_list.delete(id)
    for i in self.__column:
      del self.__datas[i][index]
  
  def delete_by_value(self, key:str, value, mode='='):
    res = self.get(key, value, mode)
    for i in res:
      self.delete(i)
  
  def change_value(self, id, **kwargs):
    index = self.__id_list[id]
    for name, data in kwargs.items():
      if self.__multi[name]:
        old = set(self.__datas[name][index])
        new = set(data)
        diff = old^new
        delete = diff&old
        add = diff&new

        for i in delete:
          self.__index[name][i].remove(id)
        for i in add:
          if type(i) != self.__types[name]:
            raise TypeError(f'The type of {name} should be {self.__types[name]}')
          if i in self.__index[name]:
            self.__index[name][i].append(id)
          else:
            self.__index[name][i] = [id]
        self.__datas[name][index] = data
        
      else:
        if type(data) != self.__types[name]:
          raise TypeError(f'The type of {name} should be {self.__types[name]}')
        if name in self.__index:
          old = self.__datas[name][index]
          self.__index[name][old].remove(id)
          if self.__index[name][data]:
            self.__index[name][data].append(id)
          else:
            self.__index[name][data] = [id]
        self.__datas[name][index] = data

  def col(self):
    return self.__column

  def add_col(self, col, data_type, multi_value=False):
    if col in self.__types:
      return 

    if multi_value == True:
      self.__types[col] = type(data_type)
      self.__multi[col] = True
      self.__column.append(col)
      self.__datas[col] = [[type(data_type)()] for _ in self.__id_list]
      self.__index[col] = BpTree(16)
      self.__index[col][type(data_type)()] = [i for i in self.__id_list]
    else:
      self.__types[col] = type(data_type)
      self.__multi[col] = False
      self.__column.append(col)
      self.__datas[col] = [type(data_type)() for _ in self.__id_list]
  
  def del_col(self, col):
    del self.__types[col]
    del self.__datas[col]
    del self.__multi[col]
    self.__column.remove(col)
    
    if col in self.__index:
      del self.__index[col]

  def create_index(self, key:str):
    if key in self.__index:
      return 
    self.__index[key] = BpTree(16)
    self.__index[key][self.__types[key]()] = []
    index = self.__index[key]
    
    for id, i in self.__id_list.items():
      value = self.__datas[key][i]
      if value in index:
        index[value].append(id)
      else:
        index[value] = [id]

  def delete_index(self, key:str):
    del self.__index[key]