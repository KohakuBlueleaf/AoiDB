from aoidb.interface import InterfaceServer as IS
from json import load

server = IS()
with open('config.json','r') as f:
  config = load(f)

server.run_with_config(config)
