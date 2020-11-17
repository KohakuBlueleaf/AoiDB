import os,sys
from os import listdir
from distutils.core import setup

if sys.platform=='win32':
	package_path = 'Lib/site-packages/'
else:
	package_path = 'site-packages/'

all_files = {}
data_files = []
queue = []

path = 'aoidb/template/'
for path, dirs, files in os.walk(path):
	if files:
		files = [path+'/'+file for file in files]
		print((package_path+path, files))
		data_files.append((package_path+path, files))

setup(
	name = 'aoidb',
	packages = ['aoidb'],
	data_files=data_files,
	version = '1.0',
	description = 'A simple database implementation in pure python',
	author = 'BlueLeaf',
	author_email = 'apolloyeh0123@gmail.com',
	keywords = ['DataBase'],
)
