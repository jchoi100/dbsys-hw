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
aggSumSchema = DBSchema('revenue', [('revenue', 'double')])
lOrderKeySchema = DBSchema('orderkey',  [('L_ORDERKEY', 'int')])
oOrderKeySchema = DBSchema('orderkey2',  [('O_ORDERKEY', 'int')])
oCustKeySchema = DBSchema('custkey',  [('O_CUSTKEY', 'int')])
cCustKeySchema = DBSchema('custkey2',  [('C_CUSTKEY', 'int')])

query3 = db.query().fromTable('customer') \
				   .join(db.query().fromTable('orders') \
				   		.join(db.query().fromTable('lineitem'), \
							  method = 'nested-loops', \
							  lhsKeySchema = oOrderKeySchema, \
							  rhsKeySchema = lOrderKeySchema, \
							  expr = 'L_ORDERKEY == O_ORDERKEY'),\
						  method = 'nested-loops', \
						  lhsKeySchema = cCustKeySchema, \
						  rhsKeySchema = oCustKeySchema, \
					          expr = 'O_CUSTKEY == C_CUSTKEY') \
				   .where('O_ORDERDATE < 19950315 and L_SHIPDATE > 19950315') \
				   .groupBy( \
				   	  groupSchema = keySchema, \
				   	  aggSchema = aggSumSchema, \
				   	  groupExpr = (lambda e: (e.L_ORDERKEY, e.O_ORDERDATE, e.O_SHIPPRIORITY)), \
				   	  aggExprs = [(0, lambda acc, e:acc + e.L_EXTENDEDPRICE * (1 - e.L_DISCOUNT), lambda x: x)], \
				   	  groupHashFn = (lambda gbVal: hash((e.L_ORDERKEY, e.O_ORDERDATE, e.O_SHIPPRIORITY)) % 31)) \
				   .select({'L_ORDERKEY': ('L_ORDERKEY', 'int'),
				   			'revenue': ('revenue', 'double'),
                          	'O_ORDERDATE': ('O_ORDERDATE', 'int'),
				   			'O_SHIPPRIORITY':('O_SHIPPRIORITY', 'int')
				   			}).finalize()

"""
Un-Optimized
"""
print("\n")
print("Un-Optimized Explain: ")
print(query3.explain())
# print("Un-Optimized Results: ")
# qresults = [query3.schema().unpack(tup) \
#        for page in db.processQuery(query3) \
#        for tup in page[1]]
# print(qresults)

"""
Pushdown Option
"""
optimized_query = db.optimizer.pushdownOperators(query3)
print("\n")
print("Pushdown Explain: ")
print(optimized_query.explain())
# print("Optimized Results: ")
# opt_qresults = [optimized_query.schema().unpack(tup) \
#         for page in db.processQuery(optimized_query) \
#         for tup in page[1]]
# print(opt_qresults)

"""
Join Order Option
"""
joined_query = db.optimizer.pickJoinOrder(optimized_query)
print("\n")
print("Join explain: ")
print(joined_query.explain())

"""
Ultimate optimizer
"""
# print("Optimized Results: ")
# optimized_query = db.optimizer.pushdownOperators(query3)
# qresults = [optimized_query.schema().unpack(tup) \
#         for page in db.processQuery(optimized_query) \
#         for tup in page[1]]
# print(qresults)