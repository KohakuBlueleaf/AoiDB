import os,sys
import setuptools


setuptools.setup(
	name = 'aoi_database',
	packages = ['aoidb'],
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
	include_package_data=True,
	zip_safe = False,
)
