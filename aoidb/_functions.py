decode = lambda x: bytes.decode(x)
encode = lambda x: bytes(x, encoding='utf-8')

def get_str(s, limit=10):
	'''
	自製format用來限制長度並判斷全半形
	'''
	s = str(s)
	res = ''
	amount = 0
	for i in s:
		amount += 1 if ord(i)<12288 else 2
		if amount>limit:
			amount -= 1 if ord(i)<12288 else 2
			break
		res += i
	
	res = ' '*(limit-amount) + res
	return res
	
class ByteObject:
	def __init__(self, data=b''):
		self.data = data
		
	def write(self, data):
		self.data += data
	
	def load(self):
		with open('./temp', 'wb') as f:
			f.write(self.data)
		with open('./temp', 'rb') as f:
			res = load(f)
		
		os.remove('./temp')
		return res
				
	def dump(self, item):
		dump(item,self)
		return self.data
