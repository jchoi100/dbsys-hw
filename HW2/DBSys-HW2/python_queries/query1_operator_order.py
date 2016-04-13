import Database
import time

db = Database.Database(dataDir='./data')

"""
Query 1
select p.p_name, s.s_name
from part p, supplier s, partsupp ps
where p.p_partkey = ps.ps_partkey
  and ps.ps_suppkey = s.s_suppkey
  and ps.ps_availqty = 1
union all
select p.p_name, s.s_name
from part p, supplier s, partsupp ps
where p.p_partkey = ps.ps_partkey
  and ps.ps_suppkey = s.s_suppkey
  and ps.ps_supplycost < 5; 
"""

start_time = time.time()

query1 = db.query().fromTable('part') \
                   .join(db.query().fromTable('partsupp') \
                         .join(db.query().fromTable('supplier'), \
                               method = 'block-nested-loops', \
                               expr = 'S_SUPPKEY == PS_SUPPKEY'), \
                         method = 'block-nested-loops', \
                         expr = 'PS_PARTKEY == P_PARTKEY') \
                   .where('PS_AVAILQTY == 1') \
                   .select({'p_name' : ('P_NAME', 'char(55)'), 's_name' : ('S_NAME', 'char(25)')}) \
        .union( \
         db.query().fromTable('part') \
                   .join(db.query().fromTable('partsupp') \
                         .join(db.query().fromTable('supplier'), \
                               method = 'block-nested-loops', \
                               expr = 'S_SUPPKEY == PS_SUPPKEY'), \
                         method = 'block-nested-loops', \
                         expr = 'PS_PARTKEY == P_PARTKEY') \
                   .where('PS_SUPPLYCOST < 5') \
                   .select({'p_name' : ('P_NAME', 'char(55)'), 's_name' : ('S_NAME', 'char(25)')}) \
        ).finalize()

end_time = time.time()

q1results = [query1.schema().unpack(tup) \
				for page in db.processQuery(query1) \
				for tup in page[1]]
print(q1results)

print(end_time - start_time)
