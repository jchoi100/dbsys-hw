-- Find the top 10 parts that with the highest quantity in returned orders. 
-- An order is returned if the returnflag field on any lineitem part is the character R.
-- Output schema: (part key, part name, quantity returned)
-- Order by: by quantity returned, descending.

-- Student SQL code here:

SELECT l_partkey, p_name, sum(l_quantity) as int_sum
FROM part, lineitem
WHERE part.p_partkey = lineitem.l_partkey and lineitem.l_returnflag = 'R'
GROUP BY l_partkey
ORDER BY int_sum DESC
LIMIT 10;
