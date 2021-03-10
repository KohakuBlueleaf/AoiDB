from aoidb.database import AoiDB2
from math import log,log2,log10

log_table = AoiDB2('Log Table')
log_table.add_col('num', int())
log_table.add_col('ln', float())
log_table.add_col('log', float())
log_table.add_col('log2', float())

log_table.create_index('num')
log_table.create_index('ln')
log_table.create_index('log')
log_table.create_index('log2')

for i in range(1,10001):
  log_table.add_data(num=i, ln=log(i), log=log10(i), log2=log2(i))

log_table.save()