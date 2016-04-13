-- For each of the top 5 nations with the greatest value (i.e., total price) of orders placed,
-- find the top 5 nations which supply these orders.
-- Output schema: (Order placer name, Order supplier name, value of orders placed)
-- Order by: (Order placer, Order supplier)

-- Notes
--  1) We are expecting exactly 25 results 

-- Student SQL code here:

DROP VIEW IF EXISTS nation_order; 
CREATE VIEW nation_order AS
    SELECT n_name, n_nationkey, sum(O.o_totalprice) AS SUMS
    FROM nation N, customer C, orders O
    WHERE N.n_nationkey = C.c_nationkey and C.c_custkey = O.o_custkey
    GROUP BY n_nationkey
    ORDER BY SUMS;

DROP VIEW IF EXISTS top_five_nations; 
CREATE VIEW top_five_nations AS
    SELECT L.n_name, L.n_nationkey, count(R.SUMS) AS RANK
    FROM nation_order L, nation_order R
    WHERE (L.SUMS = R.SUMS and L.n_nationkey = R.n_nationkey) or L.SUMS < R.SUMS
    GROUP BY L.n_nationkey, L.SUMS
    HAVING RANK <= 5
    ORDER BY L.SUMS ASC;

DROP VIEW IF EXISTS order_supplier; 
CREATE VIEW order_supplier AS
    SELECT o_orderkey, s_suppkey, s_nationkey, n_name, p_retailprice
    FROM orders, lineitem, supplier, nation, part
    WHERE o_orderkey = l_orderkey
          and l_suppkey = s_suppkey
          and s_nationkey = n_nationkey
          and l_partkey = p_partkey;

DROP VIEW IF EXISTS order_nations; 
CREATE VIEW order_nations AS
    SELECT n_name, n_nationkey, o_custkey, o_orderkey
    FROM nation, customer, orders
    WHERE n_nationkey = c_nationkey and c_custkey = o_custkey;

DROP VIEW IF EXISTS nation_suppliers; 
CREATE VIEW nation_suppliers AS
    SELECT N.n_name AS Onation, O.n_name AS Snation, sum(p_retailprice) AS TOTALSUM
    FROM order_nations N, order_supplier O
    WHERE N.o_orderkey = O.o_orderkey and N.n_nationkey IN (SELECT n_nationkey FROM top_five_nations)
    GROUP BY Onation, Snation;

DROP VIEW IF EXISTS top_five_results;
CREATE VIEW top_five_results AS
    SELECT L.Onation, L.Snation, L.TOTALSUM
    FROM nation_suppliers L, nation_suppliers R
    WHERE (L.Onation = R.Onation and L.TOTALSUM < R.TOTALSUM) 
          or (L.Onation = R.Onation and L.TOTALSUM = R.TOTALSUM and L.Snation = R.Snation)
    GROUP BY L.Onation, L.Snation, L.TOTALSUM
    HAVING count(R.TOTALSUM) <= 5
    ORDER BY L.Onation, R.Snation;

SELECT *
FROM top_five_results;