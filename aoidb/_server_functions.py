import struct
from pickle import loads,dumps

def log_error(err):
  errors = err.split('\n\n')[0].strip().split('\n')
  if errors[2].count('getattr'):
    line = 3
  else:
    line = 1

  err_file, err_line, err_pos = errors[line].strip().split(', ')
  err_program = errors[line+1].strip()
  err_cls, err_mes = errors[-1].split(': ',1)
  return ''.join(('====Error Occured====\n',
              'Error File   : {}\n'.format(err_file.split()[-1]),
              'Error Line   : {}\n'.format(err_line.split()[-1]),
              'Error Pos    : {}\n'.format(err_pos.split()[-1]),
              'Error program: {}\n'.format(err_program),
              'Error Class  : {}\n'.format(err_cls),
              'Error Message: {}\n'.format(err_mes),
              '====================='))

async def send(writer, data):
  data = dumps(data)
  data = struct.pack('>I', len(data)) + data
  writer.write(data)

async def recv_all(reader, n):
  # Helper function to recv n bytes or return None if EOF is hit
  data = b''
  while len(data) < n:
    packet = (await reader.read(n - len(data)))
    if not packet:
      return None
    data += packet
  return data

async def recv(reader):
  raw_msglen = (await recv_all(reader, 4))
  if not raw_msglen: 
    return None
  
  msglen = struct.unpack('>I', raw_msglen)[0]
  data = (await recv_all(reader, msglen))
  return loads(data)