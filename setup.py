import os,sys
from os import listdir
from setuptools import setup,find_packages


data_path = 'aoidb/template/'
data_files = [(path, [path+'/'+file for file in files]) for path, dirs, files in os.walk(data_path)]

setup(
	name = 'aoidb',
	packages = find_packages(),
	data_files = data_files,
	version = '1.0',
	description = 'A simple database implementation in pure python',
	author = 'BlueLeaf',
	author_email = 'apolloyeh0123@gmail.com',
	keywords = ['DataBase'],
	entry_points={
		'console_scripts': [
			'aoidb=aoidb.__main__:main',
		]
	},
	include_package_data=True,
	zip_safe = False,
)
