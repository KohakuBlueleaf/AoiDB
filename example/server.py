from json import load,dumps
from subprocess import Popen

with open('config.json','r') as f:
  configs = load(f)

commands = ''"from aoidb.server import run_server;\
            run_server({})"'
p_list = []
for config in configs:
  config_str = dumps(config,ensure_ascii=False)
  p_list.append(Popen(['python','-c',commands.format(config_str)]))

try:
  input()
except KeyboardInterrupt:
  pass
for process in p_list:
  process.terminate()
  while not process.poll():
    pass