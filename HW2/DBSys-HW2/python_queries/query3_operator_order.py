import Database
from Catalog.Schema  import DBSchema

db = Database.Database(dataDir='./data')

"""
Query 3
with temp as (
   select n.n_name as nation, p.p_name as part, sum(l.l_quantity) as num
   from customer c, nation n, orders o, lineitem l, part p
   where c.c_nationkey = n.n_nationkey
     and c.c_custkey = o.o_custkey
     and o.o_orderkey = l.l_orderkey
     and l.l_partkey = p.p_partkey
   group by n.n_name, p.p_name
)
select nation, max(num)
from temp
group by nation; 
"""

keySchema = DBSchema('groupByKey', [('N_NAME', 'char(25)'), ('P_NAME', 'char(55)')])
aggSumSchema = DBSchema('num', [('num', 'int')])

query3 = db.query().fromTable('part') \
				.join(db.query().fromTable('lineitem') \
					.join(db.query().fromTable('orders') \
						.join(db.query().fromTable('customer') \
							.join(db.query().fromTable('nation'), \
								  method = 'block-nested-loops', \
								  expr = 'N_NATIONKEY == C_NATIONKEY'), \
							  method = 'block-nested-loops', \
							  expr = 'C_CUSTKEY == O_CUSTKEY'), \
						  method = 'block-nested-loops', \
						  expr = 'L_ORDERKEY == O_ORDERKEY'), \
					  method = 'block-nested-loops', \
					  expr = 'P_PARTKEY == L_PARTKEY') \
				.groupBy( \
					groupSchema = keySchema, \
					aggSchema = aggSumSchema, \
					groupExpr = (lambda e: (e.N_NAME, e.P_NAME)), \
					aggExprs = [(0, lambda acc, e: acc + e.l_quantity, lambda x: x)], \
					groupHashFn = (lambda gbVal: hash(gbVal) % 13)
				) \
				.select({'nation' : ('N_NAME', 'char(25)'), \
			  		   'part' : ('P_NAME', 'char(55)'), \
			  		   'num' : ('sum', 'int')}) \
			.finalize()

q3results = [query3.schema().unpack(tup) \
				for page in db.processQuery(query3) \
				for tup in page[1]]
print(q3results)