import Database
from Catalog.Schema import DBSchema

db = Database.Database(dataDir='./data')

"""
Query 1
select p.p_name, s.s_name
from part p, supplier s, partsupp ps
where p.p_partkey = ps.ps_partkey
  and ps.ps_suppkey = s.s_suppkey
  and ps.ps_availqty = 1
union all
select p.p_name, s.s_name
from part p, supplier s, partsupp ps
where p.p_partkey = ps.ps_partkey
  and ps.ps_suppkey = s.s_suppkey
  and ps.ps_supplycost < 5;
"""

#hash join version of query 1

pPartKeySchema = DBSchema('partKey', [('P_PARTKEY', 'int')])
psPartKeySchema =  DBSchema('psPartKey', [('PS_PARTKEY', 'int')])

sSuppKeySchema = DBSchema('suppKey', [('S_SUPPKEY', 'int')])
psSuppKeySchema = DBSchema('psKey', [('PS_SUPPKEY', 'int')])

query1 = db.query().fromTable('part') \
                   .join(db.query().fromTable('partsupp') \
                         .join(db.query().fromTable('supplier'), \
                               method = 'hash', \
			                         lhsKeySchema = psSuppKeySchema, \
			                         rhsKeySchema = sSuppKeySchema, \
			                         lhsHashFn = 'hash(PS_SUPPKEY) % 13', \
			                         rhsHashFn = 'hash(S_SUPPKEY) % 13', \
			                   ), \
                         method = 'hash', \
			                   lhsKeySchema = pPartKeySchema, \
                         rhsKeySchema = psPartKeySchema, \
                         lhsHashFn = 'hash(P_PARTKEY) % 13', \
                         rhsHashFn = 'hash(PS_PARTKEY) % 13',
                   ) \
                   .where('PS_AVAILQTY == 1') \
                   .select({'p_name' : ('P_NAME', 'char(55)'), \
                            's_name' : ('S_NAME', 'char(25)')}) \
        .union( \
         db.query().fromTable('part') \
                   .join(db.query().fromTable('partsupp') \
                         .join(db.query().fromTable('supplier'), \
                               method = 'hash', \
			                         lhsKeySchema = psSuppKeySchema, \
                               rhsKeySchema = sSuppKeySchema, \
                               lhsHashFn = 'hash(PS_SUPPKEY) % 13', \
                               rhsHashFn = 'hash(S_SUPPKEY) % 13', 
                          ), \
                         method = 'hash', \
                         lhsKeySchema = pPartKeySchema, \
                         rhsKeySchema = psPartKeySchema, \
                         lhsHashFn = 'hash(P_PARTKEY) % 13', \
                         rhsHashFn = 'hash(PS_PARTKEY) % 13', \
                   ) \
                   .where('PS_SUPPLYCOST < 5') \
                   .select({'p_name' : ('P_NAME', 'char(55)'), \
                            's_name' : ('S_NAME', 'char(25)')}) \
        ).finalize()

q1results = [query1.schema().unpack(tup) \
				for page in db.processQuery(query1) \
				for tup in page[1]]
print(q1results)