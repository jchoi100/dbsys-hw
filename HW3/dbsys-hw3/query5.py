import Database
from Catalog.Schema import DBSchema

db = Database.Database(dataDir='./data')

"""
select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= 19940101
        and o_orderdate < 19950101
group by
        n_name
"""

keySchema = DBSchema('groupByKey', [('N_NAME', 'char(25)')])
aggSumSchema = DBSchema('revenue', [('revenue', 'double')])

lOrderKeySchema = DBSchema('orderkey',  [('L_ORDERKEY', 'int')])
oOrderKeySchema = DBSchema('orderkey2',  [('O_ORDERKEY', 'int')])
oCustKeySchema = DBSchema('custkey',  [('O_CUSTKEY', 'int')])
cCustKeySchema = DBSchema('custkey2',  [('C_CUSTKEY', 'int')])
cNationKeySchema = DBSchema('nationkey',  [('C_NATIONKEY', 'int')])
nNationKeySchema = DBSchema('nationkey2',  [('N_NATIONKEY', 'int')])
sNationKeySchema = DBSchema('nationkey3',  [('S_NATIONKEY', 'int')])
nRegionKeySchema = DBSchema('regionkey1',  [('N_REGIONKEY', 'int')])
rRegionKeySchema = DBSchema('regionkey2',  [('R_REGIONKEY', 'int')])
sSuppKeySchema = DBSchema('suppKey', [('S_SUPPKEY', 'int')])
lSuppKeySchema = DBSchema('lsKey', [('L_SUPPKEY', 'int')])

query5 = db.query().fromTable('region') \
				.join(db.query().fromTable('nation') \
					.join(db.query().fromTable('supplier') \
						.join(db.query().fromTable('lineitem') \
							.join(db.query().fromTable('orders') \
								.join(db.query().fromTable('customer'), \
									  method = 'nested-loops', \
									  lhsKeySchema = oCustKeySchema, \
									  rhsKeySchema = cCustKeySchema, \
									  expr = 'O_CUSTKEY == C_CUSTKEY'), \
								  method = 'nested-loops', \
								  lhsKeySchema = lOrderKeySchema, \
								  rhsKeySchema = oOrderKeySchema, \
								  expr = 'O_ORDERKEY == L_ORDERKEY'), \
							  method = 'nested-loops', \
							  lhsKeySchema = sSuppKeySchema, \
							  rhsKeySchema = lSuppKeySchema, \
							  expr = 'S_SUPPKEY == L_SUPPKEY'), \
						  method = 'nested-loops', \
						  lhsKeySchema = nNationKeySchema, \
						  rhsKeySchema = sNationKeySchema, \
						  expr = 'N_NATIONKEY == S_NATIONKEY'), \
					  method = 'nested-loops', \
					  lhsKeySchema = rRegionKeySchema, \
					  rhsKeySchema = nRegionKeySchema, \
					  expr = 'N_REGIONKEY == R_REGIONKEY') \
				   .where('O_ORDERDATE >= 19940101 and \
				   	       O_ORDERDATE < 19950101') \
				   .groupBy( \
				   	  groupSchema = keySchema, \
				   	  aggSchema = aggSumSchema, \
				   	  groupExpr = (lambda e: e.n_name), \
				   	  aggExprs = [(0, lambda acc, e:acc + e.l_extendedprice * (1 - e.l_discount), lambda x: x)], \
				   	  groupHashFn = (lambda gbVal: hash(e.n_name) % 31)) \
				   .select({'N_NAME': ('N_NAME', 'char(25)'),
				   			'revenue': ('revenue', 'double')
				   			}).finalize()

print("Un-Optimized Explain: ")
print(query5.explain())
# print("Un-Optimized Results: ")
# qresults = [query5.schema().unpack(tup) \
#         for page in db.processQuery(query5) \
#         for tup in page[1]]
# print(qresults)

"""
Pushdown Option
"""
optimized_query = db.optimizer.pushdownOperators(query5)

#print("\n")
#print("Optimized Explain: ")
#print(optimized_query.explain())
# print("Optimized Results: ")

# opt_qresults = [optimized_query.schema().unpack(tup) \
#         for page in db.processQuery(optimized_query) \
#         for tup in page[1]]
# print(opt_qresults)

"""
Join Order Option
"""
join_optimized_query = db.optimizer.pickJoinOrder(optimized_query)
print("Join optimized:\n")
for i in db.optimizer.joinList:
  print(i.explain())
print(join_optimized_query.explain())
