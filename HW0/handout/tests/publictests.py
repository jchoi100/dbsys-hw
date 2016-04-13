###########################################################
# Public tests for HW0
###########################################################
from utils import runCommand, runSQLScript, runSQLCommand
from dbsys_hw0.python.warmup import Orders, Lineitem, readCsvFile, writeBinaryFile, readBinaryFile
import common
import unittest
import os

dbName = 'test.db'
correctDir = './tests/correct_results'

class Hw0PublicTests(unittest.TestCase):
  ###########################################################
  # SQL Assignment tests
  ###########################################################
  # Table names and their cardinalities
  tables = {
    'part': 20000,
    'supplier': 1000,
    'partsupp': 80000,
    'customer': 15000,
    'orders': 150000,
    'lineitem': 600572,
    'nation': 25,
    'region': 5
  }

  # Check that each table has been created using sqlite command: '.schema <table>'
  def testCreateDB(self):
    for tbl in self.tables:
      output = runSQLCommand(dbName, '.schema %s' % tbl)
      self.assertNotEqual(len(output), 0, 'Table "%s" is not in the sqlite db.' % tbl)

  # Check that each table has the correct cardinality using sql: 'select count(*) from <table>'
  def testImport(self):
    for tbl in self.tables:
      sql = 'SELECT COUNT(*) FROM %s' % tbl
      c = int(runSQLCommand(dbName, sql))
      self.assertEqual(c, self.tables[tbl], 'Table "%s" has the wrong cardinality.' % tbl)

  def testQ1(self):
    common.checkQuery(self, 'q1')

  def testQ2(self):
    common.checkQuery(self, 'q2')

  ###########################################################
  # Python Assignment tests
  ###########################################################
  def testConstruction(self):
    # Orders
    o = Orders('100', '200', 'R', '1.0', '1998-01-01', 'A', 'A', '100', 'COMMENT')
    self.assertEqual(o.o_orderkey, 100)
    self.assertEqual(o.o_orderdate, b'1998-01-01')
    self.assertEqual(o.o_comment, b'COMMENT')

    # Lineitem
    l = Lineitem('100', '200', '300', '400', '5.0', '1.0', '1.0', '1.0', 'R', 'S', '1999-01-01', '1999-01-02', '1999-01-03', 'NONE', 'M', 'COMMENT')
    self.assertEqual(l.l_orderkey, 100)
    self.assertEqual(l.l_shipdate, b'1999-01-01')
    self.assertEqual(l.l_comment, b'COMMENT')

  def testPack(self):
    # Orders
    o = Orders('100', '200', 'R', '1.0', '1998-01-01', 'A', 'A', '100', 'COMMENT')
    byts = o.pack()
    self.assertEqual(len(byts), 139, 'Packed orders tuple has the wrong length')
   
    # Lineitem 
    l = Lineitem('100', '200', '300', '400', '5.0', '1.0', '1.0', '1.0', 'R', 'S', '1999-01-01', '1999-01-02', '1999-01-03', 'NONE', 'M', 'COMMENT')
    byts = l.pack()
    self.assertEqual(len(byts), 143, 'Packed lineitem tuple has the wrong length')
  
  def testUnpack(self):
    # Orders
    o = Orders('100', '200', 'R', '1.0', '1998-01-01', 'A', 'A', '100', 'COMMENT')
    byts = o.pack()
    o2 = Orders.unpack(byts)
    self.assertEqual(o2.o_orderkey, o.o_orderkey, 'Unpacked orders tuple has incorrect values')
    self.assertEqual(o2.o_orderdate, o.o_orderdate, 'Unpacked orders tuple has incorrect values')
    
    # Lineitem 
    l = Lineitem('100', '200', '300', '400', '5.0', '1.0', '1.0', '1.0', 'R', 'S', '1999-01-01', '1999-01-02', '1999-01-03', 'NONE', 'M', 'COMMENT')
    byts = l.pack()
    l2 = Lineitem.unpack(byts)
    self.assertEqual(l2.l_orderkey, l.l_orderkey, 'Unpacked lineitem tuple has incorrect values')
    self.assertEqual(l2.l_shipdate, l.l_shipdate, 'Unpacked lineitem tuple has incorrect values')

  def testReadCSV(self):
    # Orders
    orders_lst = readCsvFile('/home/cs416/datasets/hw0/tpch-sf0.1/orders.csv', Orders)
    for (i, order) in enumerate(orders_lst[0:5]):
      self.assertEqual(order.o_orderkey, i+1, 'Failed to read orders object from csv')
   
    # Lineitem 
    lineitem_lst = readCsvFile('/home/cs416/datasets/hw0/tpch-sf0.1/lineitem.csv', Lineitem)
    for lineitem in lineitem_lst[0:5]:
      self.assertEqual(lineitem.l_orderkey, 1, 'Failed to read lineitem object from csv')

  def testBinary(self):
    # Get some data
    orders_lst1 = readCsvFile('/home/cs416/datasets/hw0/tpch-sf0.1/orders.csv', Orders)[0:5]
    
    # Write out binary
    writeBinaryFile('orders.bin', orders_lst1)

    # Read back in, and compare
    orders_lst2 = readBinaryFile('orders.bin', Orders)
    self.assertEqual(len(orders_lst1), len(orders_lst2))
    for (o1, o2) in zip(orders_lst1, orders_lst2):
      self.assertEqual(o1.o_orderkey, o2.o_orderkey, 'Failed to read orders object from binary file')
