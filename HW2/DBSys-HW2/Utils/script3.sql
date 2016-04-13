with temp as (
   select n.n_name as nation, p.p_name as part, sum(l.l_quantity) as num
   from customer c, nation n, orders o, lineitem l, part p
   where c.c_nationkey = n.n_nationkey
     and c.c_custkey = o.o_custkey
     and o.o_orderkey = l.l_orderkey
     and l.l_partkey = p.p_partkey
   group by n.n_name, p.p_name
)
select nation, max(num)
from temp
group by nation;