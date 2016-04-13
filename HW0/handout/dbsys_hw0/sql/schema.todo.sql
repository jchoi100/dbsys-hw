-- This script creates each of the TPCH tables using the SQL 'create table' command.
drop table if exists part;
drop table if exists supplier;
drop table if exists partsupp;
drop table if exists customer;
drop table if exists orders;
drop table if exists lineitem;
drop table if exists nation;
drop table if exists region;

-- Notes:
--   1) Use all lowercase letters for table and column identifiers.
--   2) Use only INTEGER/REAL/TEXT datatypes. Use TEXT for dates.
--   3) Do not specify any integrity contraints (e.g., PRIMARY KEY, FOREIGN KEY).

-- Students should fill in the followins statements:

create table part (
    p_partkey INTEGER, p_name TEXT (55), p_mfgr TEXT (25),
    p_brand TEXT (10), p_type TEXT (25), p_size INTEGER,
    p_container TEXT (10), p_retailprice REAL, p_comment TEXT (23)
);

create table supplier (
    s_suppkey INTEGER, s_name TEXT (25), s_address TEXT (40),
    s_nationkey INTEGER, s_phone TEXT (15), s_acctbal REAL,
    s_comment TEXT (101)
);

create table partsupp (
    ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty INTEGER,
    ps_supplycost REAL, ps_comment TEXT (199)
);

create table customer (
    c_custkey INTEGER, c_name TEXT (25), c_address TEXT (40),
    c_nationkey INTEGER, c_phone TEXT (15), c_accbal REAL,
    c_MKTSEGMENT TEXT (10), c_comment TEXT (117)
);

create table orders (
    o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus TEXT (1),
    o_totalprice REAL, o_orderdate TEXT (10), o_orderpriority TEXT (15),
    o_clerk TEXT (15), o_shippriority INTEGER, o_comment TEXT (79)
);

create table lineitem (
    l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER,
    l_linenumber INTEGER, l_quantity REAL, l_extendedprice REAL,
    l_discount REAL, l_tax REAL, l_returnflag TEXT (1),
    l_linestatus TEXT (1), l_shipdate TEXT (10), l_commitdate TEXT (10),
    l_receiptdate TEXT (10), l_shipinstruct TEXT (25),
    l_shipmode TEXT (10), l_comment TEXT (44)
);

create table nation (
    n_nationkey INTEGER, n_name TEXT (25), n_regionkey INTEGER,
    n_comment TEXT (152)
);

create table region (
    r_regionkey INTEGER, r_name TEXT (25), r_comment TEXT (152)
);
