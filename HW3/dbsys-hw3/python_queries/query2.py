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
aggSumSchema = DBSchema('promo_revenue', [('promo_revenue', 'int')])
leftKeySchema = DBSchema('partkey1', [('L_PARTKEY', 'int')])
rightKeySchema = DBSchema('partkey2', [('P_PARTKEY', 'int')])

query2 = db.query().fromTable('lineitem')\
				   .join(db.query().fromTable('part'), \
				   	method = 'hash', \
				   	lhsHashFn = 'hash(L_PARTKEY) % 31', \
				   	lhsKeySchema = leftKeySchema, \
				   	rhsHashFn = 'hash(P_PARTKEY) % 31', \
				   	rhsKeySchema = rightKeySchema, \
					) \
				   .where('L_SHIPDATE >= 19950901 and \
				   	       L_SHIPDATE < 19951001')\
				   .groupBy(\
				   	  groupSchema = keySchema, \
				   	  aggSchema = aggSumSchema, \
				   	  groupExpr = (lambda e: 0), \
				   	  aggExprs = [(0, lambda acc, e:acc + l_extendedprice * (1 - l_discount), lambda x: x)], \
				   	  groupHashFn = (lambda gbVal: 0)) \
				   .select({'promo_revenue': ('promo_revenue', 'int')}).finalize()

"""
Optimization Option
"""
# query2 = db.optimizer.optimizeQuery(query2)
"""
"""

q2results = [query2.schema().unpack(tup) \
        for page in db.processQuery(query2) \
        for tup in page[1]]
print(q2results)