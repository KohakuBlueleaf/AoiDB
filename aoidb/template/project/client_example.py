from aoidb.client import DataBaseClient

DB = DataBaseClient('127.0.0.1',38738)

print(DB)
DB.lock()
DB.add_col('uid', int())
DB.add_col('u_name', str())
DB.add_col('data', str())
DB.unlock()
print(DB)

DB.lock()
DB.add_data(uid=1, u_name='Test1', data='11111')
DB.add_data(uid=2, u_name='Test2', data='22222')
DB.add_data(uid=3, u_name='Test3', data='33333')
DB.unlock()
print(DB)