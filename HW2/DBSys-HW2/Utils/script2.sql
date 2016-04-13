select part.p_name, count(*) as count
from part, lineitem
where part.p_partkey = lineitem.l_partkey and lineitem.l_returnflag = 'R'
group by part.p_name;