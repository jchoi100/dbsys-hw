import unittest
import os
import sys

import common
from publictests import Hw0PublicTests
import publictests

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print("Usage: %s [db-file]")
    sys.exit(1)

  if not os.path.exists(sys.argv[1]):
    print("Error: no database file found at: %s" % sys.argv[1])
    sys.exit(1)

  common.dbName = sys.argv[1]
  publictests.dbName = sys.argv[1]

  unittest.main(argv=[sys.argv[0], '-v'])
