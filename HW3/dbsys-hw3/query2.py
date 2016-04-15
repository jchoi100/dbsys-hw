import Database
from Catalog.Schema import DBSchema

db = Database.Database(dataDir='./data')

"""
select
        sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
        lineitem,
        part
where
        l_partkey = p_partkey
        and l_shipdate >= 19950901
        and l_shipdate < 19951001
"""

keySchema = DBSchema('att', [('att', 'int')])
aggSumSchema = DBSchema('promo_revenue', [('promo_revenue', 'double')])
leftKeySchema = DBSchema('partkey1', [('L_PARTKEY', 'int')])
rightKeySchema = DBSchema('partkey2', [('P_PARTKEY', 'int')])

query2 = db.query().fromTable('lineitem')\
				   .join(db.query().fromTable('part'), \
				   	method = 'nested-loops', \
                                         expr = 'L_PARTKEY == P_PARTKEY',\
				   	lhsKeySchema = leftKeySchema, \
				   	rhsKeySchema = rightKeySchema, \
					) \
				   .where('L_SHIPDATE >= 19950901 and \
				   	       L_SHIPDATE < 19951001').finalize()
"""\
				   .groupBy(\
				   	  groupSchema = keySchema, \
				   	  aggSchema = aggSumSchema, \
				   	  groupExpr = (lambda e: 0), \
				   	  aggExprs = [(0, lambda acc, e:acc + e.l_extendedprice * (1 - e.l_discount), lambda x: x)], \
				   	  groupHashFn = (lambda gbVal: 0)) \
				   .select({'promo_revenue': ('promo_revenue', 'double')}).finalize()
"""
"""
Optimization Option
"""
optimized_query = db.optimizer.pushdownOperators(query2)


print("Un-Optimized Explain: ")
print(query2.explain())
print("Un-Optimized Results: ")
qresults = [query2.schema().unpack(tup) \
        for page in db.processQuery(query2) \
        for tup in page[1]]
print(qresults)
print("\n")
print("Optimized Explain: ")
print(optimized_query.explain())
print("Optimized Results: ")
opt_qresults = [optimized_query.schema().unpack(tup) \
        for page in db.processQuery(optimized_query) \
        for tup in page[1]]
print(opt_qresults)
