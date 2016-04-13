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

pPartKeySchema = DBSchema('partkey',  [('P_PARTKEY', 'int')])
lPartKeySchema = DBSchema('partkey2',  [('L_PARTKEY', 'int')])
lOrderKeySchema = DBSchema('orderkey',  [('L_ORDERKEY', 'int')])
oOrderKeySchema = DBSchema('orderkey2',  [('O_ORDERKEY', 'int')])
oCustKeySchema = DBSchema('custkey',  [('O_CUSTKEY', 'int')])
cCustKeySchema = DBSchema('custkey2',  [('C_CUSTKEY', 'int')])
cNationKeySchema = DBSchema('nationkey',  [('C_NATIONKEY', 'int')])
nNationKeySchema = DBSchema('nationkey2',  [('N_NATIONKEY', 'int')])

query3 = db.query().fromTable('nation') \
				.join(db.query().fromTable('customer') \
					.join(db.query().fromTable('orders') \
						.join(db.query().fromTable('lineitem') \
							.join(db.query().fromTable('part'), \
								  method = 'hash', \
								  lhsKeySchema = lPartKeySchema, \
								  rhsKeySchema = pPartKeySchema, \
								  lhsHashFn = 'hash(L_PARTKEY) % 13', \
								  rhsHashFn = 'hash(P_PARTKEY) % 13'), \
							  method = 'hash', \
							  lhsKeySchema = oOrderKeySchema, \
							  rhsKeySchema = lOrderKeySchema, \
							  lhsHashFn = 'hash(O_ORDERKEY) % 13', \
							  rhsHashFn = 'hash(L_ORDERKEY) % 13'), \
						  method = 'hash', \
						  lhsKeySchema = cCustKeySchema, \
						  rhsKeySchema = oCustKeySchema, \
						  lhsHashFn = 'hash(C_CUSTKEY) % 13', \
						  rhsHashFn = 'hash(O_CUSTKEY) % 13'), \
					  method = 'hash', \
					  lhsKeySchema = nNationKeySchema, \
					  rhsKeySchema = cNationKeySchema, \
					  lhsHashFn = 'hash(N_NATIONKEY) % 13', \
					  rhsHashFn = 'hash(C_NATIONKEY) % 13') \
				.groupBy( \
					groupSchema = keySchema, \
					aggSchema = aggSumSchema, \
					groupExpr = (lambda e: (e.N_NAME2, e.P_NAME2)), \
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



#print(query3.schema().toString())