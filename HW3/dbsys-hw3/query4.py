import Database
from Catalog.Schema import DBSchema

db = Database.Database(dataDir='./data')

"""
select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
from
        customer,
        orders,
        lineitem,
        nation
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= 19931001
        and o_orderdate < 19940101
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
"""

keySchema = DBSchema('groupByKey', [('C_CUSTKEY', 'int'), ('C_NAME', 'char(25)'), \
									('C_ACCTBAL', 'double'), ('C_PHONE', 'char(15)'), \
									('N_NAME', 'char(25)'), ('C_ADDRESS', 'char(40)'), \
									('C_COMMENT', 'char(117)')])
aggSumSchema = DBSchema('revenue', [('revenue', 'double')])

lOrderKeySchema = DBSchema('orderkey',  [('L_ORDERKEY', 'int')])
oOrderKeySchema = DBSchema('orderkey2',  [('O_ORDERKEY', 'int')])
oCustKeySchema = DBSchema('custkey',  [('O_CUSTKEY', 'int')])
cCustKeySchema = DBSchema('custkey2',  [('C_CUSTKEY', 'int')])
cNationKeySchema = DBSchema('nationkey',  [('C_NATIONKEY', 'int')])
nNationKeySchema = DBSchema('nationkey2',  [('N_NATIONKEY', 'int')])

query4 = db.query().fromTable('nation') \
				.join(db.query().fromTable('customer') \
					.join(db.query().fromTable('orders') \
						.join(db.query().fromTable('lineitem'), \
							  method = 'nested-loops', \
							  lhsKeySchema = oOrderKeySchema, \
							  rhsKeySchema = lOrderKeySchema, \
							  expr = 'L_ORDERKEY == O_ORDERKEY'), \
						  method = 'nested-loops', \
						  lhsKeySchema = cCustKeySchema, \
						  rhsKeySchema = oCustKeySchema, \
						  expr = 'O_CUSTKEY == L_CUTKEY'), \
					  method = 'nested-loops', \
					  lhsKeySchema = nNationKeySchema, \
					  rhsKeySchema = cNationKeySchema, \
					  expr = 'C_NATIONKEY == N_NATIONKEY') \
				   .where('O_ORDERDATE >= 19931001 and \
				   	       O_ORDERDATE < 19940101 and \
				   	       L_RETURNFLAG = \'R\'') \
				   .groupBy( \
				   	  groupSchema = keySchema, \
				   	  aggSchema = aggSumSchema, \
				   	  groupExpr = (lambda e: (e.c_custkey, e.c_name, e.c_acctbal, e.c_phone, e.n_name, e.c_address, e.c_comment)), \
				   	  aggExprs = [(0, lambda acc, e:acc + e.l_extendedprice * (1 - e.l_discount), lambda x: x)], \
				   	  groupHashFn = (lambda gbVal: hash((e.c_custkey, e.c_name, e.c_acctbal, e.c_phone, e.n_name, e.c_address, e.c_comment)) % 31)) \
				   .select({'C_CUSTKEY': ('C_CUSTKEY', 'int'),
				   			'C_NAME': ('C_NAME', 'char(25)'),
				   			'revenue': ('revenue', 'double'),
                          	'C_ACCTBAL': ('C_ACCTBAL', 'double'),
				   			'N_NAME':('N_NAME', 'char(25)'),
				   			'C_ADDRESS': ('C_ADDRESS', 'char(40)'),
							'C_PHONE': ('C_PHONE', 'char(15)'),
							'C_COMMENT': ('C_COMMENT', 'char(117)')
				   			}).finalize()

"""
Optimization Option
"""
# query4 = db.optimizer.pushdownOperators(query4)
"""
"""

q4results = [query4.schema().unpack(tup) \
        for page in db.processQuery(query4) \
        for tup in page[1]]
print(q4results)
