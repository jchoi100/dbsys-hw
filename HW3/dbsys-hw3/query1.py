import Database
import Database
from Catalog.Schema import DBSchema

db = Database.Database(dataDir='./data/data-tpch-sf-0.001')

"""
select
        sum(l_extendedprice * l_discount) as revenue
from
        lineitem
where
        l_shipdate >= 19940101
        and l_shipdate < 19950101
        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        and l_quantity < 24
"""

keySchema = DBSchema('att', [('att', 'int')])
aggSumSchema = DBSchema('revenue', [('revenue', 'int')])

query1 = db.query().fromTable('lineitem')\
				   .where('L_SHIPDATE >= 19940101 and \
				   	       L_SHIPDATE < 19950101 and \
				   	       L_DISCOUNT < 0.07 and L_DISCOUNT > 0.05 and \
				   	       L_QUANTITY < 24 ')\
				   .groupBy(\
				   	  groupSchema = keySchema, \
				   	  aggSchema = aggSumSchema, \
				   	  groupExpr = (lambda e: 0), \
				   	  aggExprs = [(0, lambda acc, e:acc + L_EXTENDEDPRICE * L_DISCOUNT, lambda x: x)], \
				   	  groupHashFn = (lambda gbVal: 0)) \
				   .select({'revenue': ('revenue', 'int')}).finalize()
"""
Optimization Option
"""
# query1 = db.optimizer.optimizeQuery(query1)
"""
"""

q1results = [query1.schema().unpack(tup) \
        for page in db.processQuery(query1) \
        for tup in page[1]]
print(q1results)
