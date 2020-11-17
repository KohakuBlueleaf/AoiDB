import os,sys
from json import load,dumps
from subprocess import Popen

with open('config.json','r',encoding='utf-8') as f:
	configs = load(f)

p_list = []
for config in configs:
	config_str = dumps(config,ensure_ascii=False)
	config_str = config_str.replace('"',"'")

	python = 'python' if sys.platform=='win32' else 'python3'
	p_list.append(Popen([python,'runner.py',config_str]))

try:
	input()
except KeyboardInterrupt:
	pass

for process in p_list:
	process.terminate()
	while not process.poll():
		pass
