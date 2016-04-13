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