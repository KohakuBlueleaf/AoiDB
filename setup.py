import os,sys
import setuptools
from Cython.Build import cythonize

datafiles = []
for path, dirs, files in os.walk('aoidb\\template'):
	datafiles.append((path, [path+'/'+i for i in files]))

print(datafiles)
setuptools.setup(
	name = 'aoi_database',
	packages = ['aoidb'],
	ext_modules = cythonize(
		"aoidb/_bp_tree.pyx",
		compiler_directives={'language_level' : "3"}
	),
	version = '1.0.0',
	description = 'A simple database implementation in pure python',
	author = 'BlueLeaf',
	author_email = 'apolloyeh0123@gmail.com',
	keywords = ['DataBase'],
	entry_points={
		'console_scripts': [
			'aoidb = aoidb.__main__: main',
		]
	},
	data_files=datafiles,
	include_package_data=True,
	zip_safe = False,
)
