-- Find the customer market segments where the yearly total number of orders declines 
-- in the last 2 years in the dataset. Note the database will have different date 
-- ranges per market segment, for example segment A records between 1990-1995, and 
-- segment B between 1992-1998. That is for segment A, we want the difference between 
-- 1995 and 1994.
-- Output schema: (market segment, last year for segment, difference in # orders)
-- Order by: market segment ASC 

-- Notes
--  1) Use the sqlite function strftime('%Y', <text>) to extract the year from a text field representing a date.
--  2) Use CAST(<text> as INTEGER) to convert text (e.g., a year) into an INTEGER.
--  3) You may use a SQL 'WITH' clause.

-- Student SQL code here:

DROP VIEW IF EXISTS yearly_count;
CREATE VIEW yearly_count AS
    SELECT c_mktsegment, cast(strftime('%Y', o_orderdate) AS INTEGER) AS year, count(*) AS total
    FROM customer, orders
    WHERE c_custkey = o_custkey
    GROUP BY c_mktsegment, year;

DROP VIEW IF EXISTS latest;
CREATE VIEW latest AS
    SELECT ycount.c_mktsegment, latest_year, total
    FROM yearly_count ycount, (SELECT c_mktsegment, cast(max(strftime('%Y', o_orderdate)) AS INTEGER) AS latest_year
                                FROM customer, orders
                                WHERE c_custkey = o_custkey
                                GROUP BY c_mktsegment) M
    WHERE ycount.c_mktsegment = M.c_mktsegment and ycount.year = M.latest_year;

DROP VIEW IF EXISTS secondlatest;
CREATE VIEW secondlatest AS
    SELECT ycount.c_mktsegment, year, total
    FROM yearly_count ycount, (SELECT c_mktsegment, cast(max(strftime('%Y', o_orderdate)) AS INTEGER) AS latest_year
                                FROM customer, orders
                                WHERE c_custkey = o_custkey
                                GROUP BY c_mktsegment) M
    WHERE ycount.c_mktsegment = M.c_mktsegment and ycount.year = (M.latest_year - 1);

DROP VIEW IF EXISTS decrease_report;
CREATE VIEW decrease_report AS
    SELECT L.c_mktsegment, L.latest_year, (S.total - L.total) as decrease
    FROM secondlatest S, latest L
    WHERE S.c_mktsegment = L.c_mktsegment AND decrease > 0
    ORDER BY L.c_mktsegment ASC;

SELECT *
FROM decrease_report;
