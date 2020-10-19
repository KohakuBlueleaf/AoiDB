import sys,os
from math import *
import AoiDB
from time import *


sys.setrecursionlimit(10000000)		

DB = AoiDB.AoiPy(name='指對數表')

DB.add_col('num', int())
DB.add_col('pow2', int())
DB.add_col('sqrt', float())
DB.add_col('log', float())
DB.add_col('log2', float())
DB.add_col('ln', float())
	
T0 = time()
for i in range(1,1001):
	print(f'{i}', end='\r')
	DB.add_data(num=i, pow2=i**2, sqrt=i**0.5, log=log(i,10), log2=log(i,2), ln=log(i))
DB.save('./指對數表')

print(DB)

'''
T1 = time()
DB.create_index('num')
DB.create_index('pow2')
DB.create_index('sqrt')
DB.create_index('log')
DB.create_index('ln')

T2 = time()
DB.save('./指對數表')

T3 = time()

print('Calculate   : {}'.format(str(T1-T0)[:5]))
print('Create Index: {}'.format(str(T2-T1)[:5]))
print('Save        : {}'.format(str(T3-T2)[:5]))'''
