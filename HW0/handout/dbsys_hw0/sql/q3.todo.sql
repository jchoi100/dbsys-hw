-- Find the name of the most heavily ordered (i.e., highest quantity) part per nation.
-- Output schema: (nation key, nation name, part key, part name, quantity ordered)
-- Order by: (nation key, part key) SC

-- Notes
--   1) You may use a SQL 'WITH' clause for common table expressions.
--   2) A single nation may have more than 1 most-heavily-ordered-part.

-- Student SQL code here:

DROP VIEW IF EXISTS sum_nation;
CREATE VIEW sum_nation AS
    SELECT N.n_nationkey, N.n_name, P.p_partkey, P.p_name, sum(L.l_quantity) AS QUANTITYSUM
    FROM nation N, customer C, orders O, lineitem L, part P
    WHERE C.c_nationkey = N.n_nationkey
          and C.c_custkey = O.o_custkey
          and O.o_orderkey = L.l_orderkey
          and P.p_partkey = L.l_partkey
    GROUP BY C.c_nationkey, L.l_partkey;

DROP VIEW IF EXISTS maxnumber;
CREATE VIEW maxnumber AS
    SELECT n_nationkey, max(QUANTITYSUM) AS MAXNUM
    FROM sum_nation
    GROUP BY n_nationkey;

DROP VIEW IF EXISTS matchnumber;
CREATE VIEW matchnumber AS
    SELECT S.n_nationkey, S.n_name, S.p_name, S.QUANTITYSUM
    FROM sum_nation S, maxnumber M
    WHERE S.n_nationkey = M.n_nationkey and S.QUANTITYSUM = M.MAXNUM;

SELECT *
FROM matchnumber;

