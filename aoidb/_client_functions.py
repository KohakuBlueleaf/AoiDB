import struct
from pickle import dumps,loads
import re

def log_error(err, ignore_file_name=[]):
  all_mes = re.findall(r'File.*\n\s+.*',err)
  err_class= re.findall(r'.+: .+',err)
  if err_class:
    err_class, err_message  = err_class.split(': ',1)
  else:
    err_class = err.strip().split('\n')[-1]
    err_message = ''

  output = '========Error Occured========\n'
  before_file = ''

  for i in all_mes:
    state, program = i.split('\n')
    err_file, err_line, err_pos = state.split(', ')

    for i in ignore_file_name:
      if err_file.find(i)!=-1:
        break
    else:
      if before_file:
        output += '-----------------------------\n'
      if err_file!=before_file:
        output += f'Error File   : {err_file[5:]}\n'
        before_file = err_file
      
      output += f'Error Line   : {err_line[5:]}\n'
      output += f'Error Pos    : {err_pos[3:]}\n'
      output += f'Error program: {program.strip()}\n'
  
  output += '=============================\n'
  output += f'Error Class  : {err_class}\n'
  output += f'Error Message: {err_message}\n'
  output += '============================='
  print(output)

def recv_msg(sock):
  raw_msglen = recvall(sock, 4)
  if not raw_msglen:
    return None
  msglen = struct.unpack('>I', raw_msglen)[0]
  return loads(recvall(sock, msglen))

def recvall(sock, n):
  data = b''
  while len(data) < n:
    packet = sock.recv(n - len(data))
    if not packet:
      return None
    data += packet
  return data

def send_with_len(sock, data):
  data = dumps(data)
  data = struct.pack('>I', len(data)) + data
  sock.sendall(data)