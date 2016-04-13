--  Find the 10 customers who spent the highest average number of days waiting for shipments.
-- A customer is waiting between a shipment's ship date and receipt date
-- Output schema: (customer key, customer name, average wait)
-- Order by: average wait DESC

-- Notes
--  1) Use the sqlite DATE(<text>) function to interpret a text field as a date.
--  2) Use subtraction to compute the duration between two dates (e.g., DATE(column1) - DATE(column2)).
--  3) Assume that a package cannot be received before it is shipped.

-- Student SQL code here:

SELECT customer.c_custkey, customer.c_name, 
       avg(DATE(lineitem.l_receiptdate) - DATE(lineitem.l_shipdate))
FROM customer, orders, lineitem
WHERE customer.c_custkey = orders.o_custkey 
      and orders.o_orderkey = lineitem.l_orderkey
GROUP BY customer.c_custkey
ORDER BY avg(DATE(lineitem.l_receiptdate) - DATE(lineitem.l_shipdate)) DESC
LIMIT 10;
