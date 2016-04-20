import Database
import Database
from Catalog.Schema import DBSchema

db = Database.Database(dataDir='./data')

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
aggSumSchema = DBSchema('revenue', [('revenue', 'double')])

query1 = db.query().fromTable('lineitem').where('L_SHIPDATE >= 19940101 and L_SHIPDATE < 19950101 and L_DISCOUNT < 0.07 and L_DISCOUNT > 0.05 and L_QUANTITY < 24 ') \
				   .groupBy(\
				   	  groupSchema = keySchema, \
				   	  aggSchema = aggSumSchema, \
				   	  groupExpr = (lambda e: 0), \
				   	  aggExprs = [(0, lambda acc, e:acc + e.L_EXTENDEDPRICE * e.L_DISCOUNT, lambda x: x)], \
				   	  groupHashFn = (lambda gbVal: 0)) \
				   .select({'revenue': ('revenue', 'double')}).finalize()


print("Un-Optimized Explain: ")
print(query1.explain())
# print("Un-Optimized Results: ")
# qresults = [query1.schema().unpack(tup) \
#         for page in db.processQuery(query1) \
#         for tup in page[1]]
# print(qresults)

"""
Pushdown Option
"""
optimized_query = db.optimizer.pushdownOperators(query1)
# optimized_query = db.optimizer.pickJoinOrder(optimized_query)
print("\n")
print("Optimized Explain: ")
print(optimized_query.explain())
# print("Optimized Results: ")
# opt_qresults = [optimized_query.schema().unpack(tup) \
#         for page in db.processQuery(optimized_query) \
#         for tup in page[1]]
# print(opt_qresults)
