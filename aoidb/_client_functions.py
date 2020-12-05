import struct
from pickle import dumps,loads

def log_error(err):
  errors = err.split('\n\n')[0].strip().split('\n')
  if errors[2].count('getattr'):
    line = 3
  else:
    line = 1

  err_file, err_line, err_pos = errors[line].strip().split(', ')
  err_program = errors[line+1].strip()
  err_cls, err_mes = errors[-1].split(': ',1)
  print(
    '====Error Occured====\n',
    'Error File   : {}\n'.format(err_file.split()[-1]),
    'Error Line   : {}\n'.format(err_line.split()[-1]),
    'Error Pos    : {}\n'.format(err_pos.split()[-1]),
    'Error program: {}\n'.format(err_program),
    'Error Class  : {}\n'.format(err_cls),
    'Error Message: {}\n'.format(err_mes),
    '=====================',
    sep=''
  )

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