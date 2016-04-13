from utils import runSQLScript, runCommand
import os

dbName = 'test.db'
correctDir = './tests/correct_results'

def setupDb():
  runCommand('rm -f %s' % dbName)
  runSQLScript(dbName, 'schema.sql')
  runSQLScript(dbName, 'import.sql')

def checkQuery(testcase, name):
 # Run the query
 script = name + '.todo.sql'
 actual = runSQLScript(dbName, script)
 # Compare to correct 
 csv = os.path.join(correctDir, name + '.csv')
 with open(csv, 'r') as f:
   expected =f.read().strip()
   testcase.assertEqual(actual, expected, 'Query "%s" produced incorrect output' % name)
