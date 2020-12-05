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
      self.temp = {}  #儲存上一次的資料，在save_change的時用來判斷有沒有修改
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
      self.temp = node.temp
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
      self.temp[key] = self.data[key]
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
        out += '{}│'.format(get_str(i))
        for j in self.column:
          out += '{}│'.format(get_str(self.id_list[i][j]))
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

  def save(self, path=''):
    if path=='' and self.path=='':
      path = self.name

    if path[-4:]!='.aoi':
      path+='.aoi'
      
    all_data = (
      self.name, 
      self.type,
      self.all_data,
      self.column, 
      [i for i in self.index.keys()],
      self.id_list, 
      self.idmax
    )
    with open(path if path!='.aoi' else self.path, 'wb') as f:
      pickle.dump(all_data, f)
  
  def load(self, path):
    if not self.path:
      self.path = path
    
    with open(path, 'rb') as f:
      all_data = pickle.load(f)
    
    self.name, self.type, self.all_data, self.column, self.index, self.id_list, self.idmax = all_data
    for data in self.all_data:
      now = data

      #to fit new type of Data
      if hash(now)!=hash(f'AoiDB_Data_{now.id}'):
        new = self.Data(now.id)
        for key,value in now.data.items():
          new.data[key] = value
      
    #to fit new type of B+Tree
    if type(self.index)==dict:
      for col, bptree in self.index.items():
        if not hasattr(bptree, 'delete'):
          new_tree = BpTree(16)
          for key, value in bptree.items():
            new_tree[key] = value if type(value)==int else value.id
          self.index[col] = new_tree
    else:
      index_col = self.index
      self.index = {}
      for col in index_col:
        new_index = BpTree(16)
        for i in self.all_data:
          new_index[i[col]] = new_index.get(i[col], []).append(i.id)

  def get_by_id(self, id):
    return self.id_list[id]
  
  def get_e(self, **kwargs):
    final = None
    for key,target in kwargs.items():
      if key in self.index:
        now = set(self.id_list[i] for i in self.index[key].get(target,[]))
      else:
        now = set([i for i in self.all_data if i[key]==target])
      if not final:
        final = now
      else:
        final &= now
    
    return self.DataSet(final)
  
  def get_l(self, **kwargs):
    final = None
    for key,target in kwargs.items():
      now = set([i for i in self.all_data if i[key]<target])
      if not final:
        final = now
      else:
        final &= now
    return self.DataSet(final)
  
  def get_g(self, **kwargs):
    final = None
    for key,target in kwargs.items():
      now = set([i for i in self.all_data if i[key]>target])
      if not final:
        final = now
      else:
        final &= now
    return self.DataSet(final)
  
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
    new = self.Data(self.idmax)
    
    self.all_data.append(new)
    self.id_list[self.idmax] = new
    self.idmax += 1
    
    for key,value in kwargs.items():
      new.data[key] = value
      
    for i in self.column:
      if i not in new.data:
        new.data[i] = self.type[i]()
        
      if i in self.index:
        if new.data[i] in self.index[i]:
          self.index[i][new.data[i]].append(new.id)
        else:
          self.index[i][new.data[i]] = [new.id]
    
      new.temp[i] = new.data[i]
    
    return new
  
  def delete(self, node:Data):
    self.all_data.remove(node)
    data = self.id_list[node.id]
    node = data
    
    for key,tree in self.index.items():
      tree[node[key]].remove(node.id)
      if tree[node[key]] == []:
        tree.delete(node[key])
    
    self.id_list.delete(node.id)
  
  def delete_by_value(self, key:str, value, mode='='):
    res = self.get(key, value, mode)
    for i in res:
      self.delete(i)
  
  def change_value(self, id, **kwargs):
    target = self.id_list[id]
    for key,value in kwargs.items():
      if key in self.index:
        self.index[key][target[key]].remove(target.id)
      target[key] = value

      if key in self.index:
        if value in self.index[key]:
          self.index[key][value].append(target.id)
        else:
          self.index[key][value] = [target.id]
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
    self.index[key] = BpTree(16)
    self.index[key][self.type[key]()] = []
    index = self.index[key]
    
    for i in self.all_data:
      value = i[key]
      if value in index:
        index[value].append(i.id)
      else:
        index[value] = [i.id]
  
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
