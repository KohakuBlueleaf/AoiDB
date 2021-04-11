from aoidb.interface import InterfaceClient

client = InterfaceClient('127.0.0.1',12345)
client.create_new('test')

test_db = client.get_reader('test')
test_db.add_col('test_str','')
test_db.add_data(test_str='This is a test data.')
print(test_db)

client.save_all()