import Database
from Catalog.Schema  import DBSchema
import time

db = Database.Database(dataDir='./data')

"""
Query 2
select part.p_name, count(*) as count
from part, lineitem
where part.p_partkey = lineitem.l_partkey and lineitem.l_returnflag = 'R'
group by part.p_name; 
"""

#try group by with supplier table-->see how it goes

keySchema = DBSchema('partkey',  [('P_NAME', 'char(55)')])
aggCountSchema = DBSchema('count', [('count', 'int')])

start_time = time.time()

query2 = db.query().fromTable('part') \
                   .join(db.query().fromTable('lineitem'), \
                         method = 'block-nested-loops', \
                         expr = 'P_PARTKEY == L_PARTKEY') \
                   .groupBy( \
                      groupSchema = keySchema, \
                      aggSchema = aggCountSchema, \
                      groupExpr = (lambda e: e.P_NAME), \
                      aggExprs = [(0, lambda acc, e:acc+1, lambda x: x)], \
                      groupHashFn = (lambda gbVal: hash(gbVal[0]) % 13) \
                    ) \
                   .where('L_RETURNFLAG == \'R\'') \
                   .finalize()

end_time = time.time()

q2results = [query2.schema().unpack(tup) \
        for page in db.processQuery(query2) \
        for tup in page[1]]

print(q2results)

print("The query took %s seconds to process!" % (end_time - start_time))