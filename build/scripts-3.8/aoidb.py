import os,sys

from argparse import ArgumentParser
from .database import AoiDB

def main():
	file_path = os.path.abspath(__file__)
	print(file_path)

if __name__=='__main__':
	main()