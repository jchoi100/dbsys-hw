import itertools
import pdb
import copy
from collections import deque
from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo
from Catalog.Schema import DBSchema

class BushyOptimizer(Optimizer):
  def __init__(self, db):
    super().__init__(db)
    self.db = db
    self.plansConsidered = 0

  #copy in the same pickJoinOrder, just increment self.plansConsidered as necessary

  def getJoins(self, plan):

    preJoin = plan
    foundJoin = False

    currNode = plan.root
    prevNode = plan.root

    while currNode is not None:
      currType = currNode.operatorType()
      prevType = prevNode.operatorType()

      print(currType)
      if foundJoin is False:
        if "Join" in currType:
          foundJoin = True
          if prevType is "Project" or prevType is "Select" or prevType is "GroupBy":
            prevNode.subPlan = None
          elif prevType is "Union":
            prevNode.rhsPlan = None
            # TODO is this supposed to be rhsPlan/lhsPlan in light of the lhs/rhs seeming to be switched?
            # TODO maybe just the python queries are in the wrong order

        elif currType is "Project" or currType is "Select" or currType is "GroupBy":
          prevNode = currNode
          if currNode.subPlan is None:
            currNode = None
          else:
            currNode = currNode.subPlan

        elif currType is "Union":
          prevNode = currNode
          if currNode.lhsPlan is None:
            currNode = None
          else:
            currNode = currNode.lhsPlan

        elif currType is "TableScan":
          return preJoin


      elif foundJoin is True:
        if "Join" in currType:
          retrieved = self.getJoins(Plan(root=currNode.lhsPlan))
          if retrieved is not None:
            if not retrieved.root.operatorType().endswith("Join"):
              self.joinList.append(retrieved.root)
              print("Recursively appended... " + self.joinList[-1].operatorType())

          self.joinList.append(currNode.lhsPlan)
          retrieved = self.getJoins(Plan(root=currNode.rhsPlan))
          if retrieved is not None:
            if not retrieved.root.operatorType().endswith("Join"):
              self.joinList.append(retrieved.root)
              print("Recursively appended... " + self.joinList[-1].operatorType())
          return None
        elif currType is "Project" or currType == "Select" or currType == "GroupBy":
          prevNode = currNode
          if currNode.subPlan is None:
            currNode = None
          else:
            currNode = currNode.subPlan

        elif currType is "Union":
          prevNode = currNode
          if currNode.lhsPlan is None:
            currNode = None
          else:
            currNode = currNode.lhsPlan

        elif currType is "TableScan":
          return preJoin

    return preJoin
