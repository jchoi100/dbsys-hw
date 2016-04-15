import Database
from Catalog.Schema import DBSchema

db = Database.Database(dataDir='./data')

"""
select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        customer,
        orders,
        lineitem
where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < 19950315
        and l_shipdate > 19950315
group by
        l_orderkey,
        o_orderdate,
        o_shippriority
"""

keySchema = DBSchema('groupByKey', [('L_ORDERKEY', 'int'), ('O_ORDERDATE', 'int'), ('O_SHIPPRIORITY', 'int')])
aggSumSchema = DBSchema('revenue', [('revenue', 'int')])
lOrderKeySchema = DBSchema('orderkey',  [('L_ORDERKEY', 'int')])
oOrderKeySchema = DBSchema('orderkey2',  [('O_ORDERKEY', 'int')])
oCustKeySchema = DBSchema('custkey',  [('O_CUSTKEY', 'int')])
cCustKeySchema = DBSchema('custkey2',  [('C_CUSTKEY', 'int')])

query3 = db.query().fromTable('customer') \
				   .join(db.query().fromTable('orders') \
				   		.join(db.query().fromTable('lineitem'), \
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
				   .where('c_mktsegment = \'BUILDING\' and \
				   	       o_orderdate < 19950315 and \
				   	       l_shipdate > 19950315') \
				   .groupBy( \
				   	  groupSchema = keySchema, \
				   	  aggSchema = aggSumSchema, \
				   	  groupExpr = (lambda e: (e.l_orderkey, e.o_orderdate, e.o_shippriority)), \
				   	  aggExprs = [(0, lambda acc, e:acc + l_extendedprice * (1 - l_discount), lambda x: x)], \
				   	  groupHashFn = (lambda gbVal: hash((e.l_orderkey, e.o_orderdate, e.o_shippriority)) % 31)) \
				   .select({'L_ORDERKEY': ('L_ORDERKEY', 'int'),
				   			'revenue': ('revenue', 'int'),
                          	'O_ORDERDATE': ('O_ORDERDATE', 'int'),
				   			'O_SHIPPRIORITY':('O_SHIPPRIORITY', 'int')
				   			}).finalize()

"""
Optimization Option
"""
# query3 = db.optimizer.optimizeQuery(query3)
"""
"""

q3results = [query3.schema().unpack(tup) \
        for page in db.processQuery(query3) \
        for tup in page[1]]
print(q3results)