###########################################################
# Command line / sqlite utils.
# These functions return the output of a shell command,
# and throw an exception upon non-zero return code.
###########################################################
import os
import subprocess

sqlDir = './dbsys_hw0/sql'

# Runs a shell command, returning the output.
def runCommand(cmd, debug=False):
  if debug:
    print("CMD: %s" % cmd)
  return subprocess.check_output(cmd, shell=True).decode().strip()

# Run a SQL script from the solution directory, by name.
def runSQLScript(dbName, scriptName):
  scriptPath = os.path.join(sqlDir, scriptName)
  if not os.path.exists(scriptPath):
    raise Exception('Failed to find SQL script at: %s' % scriptPath) 
  return runCommand('sqlite3 %s < %s' % (dbName, scriptPath))
  
# Run a raw SQL command (or sqlite meta-command).
def runSQLCommand(dbName, sql):
  return runCommand('sqlite3 %s -cmd "%s" < /dev/null' % (dbName, sql))
