import itertools
import pdb
import copy
import sys
from collections import deque
from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo
from Catalog.Schema import DBSchema
from Query.Operators.BushyOptimizer import BushyOptimizer

# Main difference from Bushy: For each run through the main for loop in pickJoinOrder
# Only one optimal plan will be found, instead a list of all the
# possible ones for the various combinations within the joinList
class GreedyOptimizer(BushyOptimizer):
  def __init__(self, db):
    super().__init__(db)
    self.db = db
    self.plansConsidered = 0

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    plan.prepare(self.db)

    preJoin = self.getJoins(plan)
    ret = Plan(root=preJoin.root)
    if len(self.joinList) is 0:
      return plan
    optimalList = list()

    for i in self.joinList:
      optimalList.append((Plan(root=i), i))

    for x in range(2, len(self.joinList) + 1):
      tempList = list()
      minCost = sys.maxsize
      for o, n in optimalList:
        for i in self.joinList:
          if not self.insidePlan(i, o):
            tempKey = frozenset([n, i])
            if n is None:
              tempKey = frozenset([o, i])

            if tempKey in self.joinExprList:
              tempJoin = Join(o.root,i, lhsSchema=o.schema(), \
              rhsSchema=i.schema(), method="block-nested-loops", expr=self.joinExprList[tempKey][0])

              # o first, BNL
              tempPlan = Plan(root=tempJoin)
              tempPlan.prepare(self.db)
              bestPlan = tempPlan
              cost = tempPlan.cost(False)
              if(cost < minCost):
                minCost = cost
                tempList.append(tempPlan)

              # i first, BNL
              tempPlan = self.swapPlan(tempPlan, False)
              tempPlan.prepare(self.db)
              cost = tempPlan.cost(False)
              if(cost < minCost):
                minCost = cost
                tempList.append(tempPlan)

              #i first, NL
              tempPlan = self.swapPlan(tempPlan, True)
              tempPlan.prepare(self.db)
              cost = tempPlan.cost(False)
              if(cost < minCost):
                minCost = cost
                tempList.append(tempPlan)

              # o first, NL
              tempPlan = self.swapPlan(tempPlan, False)
              tempPlan.prepare(self.db)
              cost = tempPlan.cost(False)
              if(cost < minCost):
                minCost = cost
                tempList.append(tempPlan)
              self.plansConsidered += 4

              oldNode = self.joinExprList[tempKey][1]
              tempList.append((bestPlan, oldNode))

      optimalList = list()
      optimalList.append(tempList[-1])

    currNode = preJoin.root
    while currNode is not None:
      currType = currNode.operatorType()
      if currType is "Project" or currType is "Select" or currType is "GroupBy":
        if currNode.subPlan is not None:
          currNode = currNode.subPlan
        else:
          currNode.subPlan = optimalList[0][0].root
      elif currType is "TableScan":
        currNode = None
      elif currType is "Union" or "Join":
        if currNode.rhsPlan is not None:
          currNode = currNode.lhsPlan
        else:
          currNode = optimalList[0][0].root

    currNode = optimalList[0][0].root
    return preJoin

  def getJoins(self, plan):

    preJoin = plan
    foundJoin = False

    currNode = plan.root
    prevNode = plan.root

    while currNode is not None:
      currType = currNode.operatorType()
      prevType = prevNode.operatorType()

      # print(currNode.explain())
      if foundJoin is False:
        if "Join" in currType:
          foundJoin = True
          if prevType is "Project" or prevType is "Select" or prevType is "GroupBy":
            prevNode.subPlan = None

          elif prevType is "Union":
            prevNode.rhsPlan = None
            prevNode.lhsPlan = None

        elif currType is "Project" or currType is "Select" or currType is "GroupBy":
          prevNode = currNode
          if currNode.subPlan is None:
            currNode = None
          else:
            currNode = currNode.subPlan

        elif currType is "Union":
          prevNode = currNode
          if currNode.rhsPlan is None:
            currNode = None
          else:
            currNode = currNode.rhsPlan

        elif currType is "TableScan":
          return preJoin


      elif foundJoin is True:
        if "Join" in currType:
          key = frozenset([currNode.rhsPlan, currNode.lhsPlan])
          expr = currNode.joinExpr
          self.joinExprList[key] = (expr, currNode)

          retrieved = self.getJoins(Plan(root=currNode.rhsPlan))
          if retrieved is not None:
            if not retrieved.root.operatorType().endswith("Join"):
              self.joinList.append(retrieved.root)
          retrieved = self.getJoins(Plan(root=currNode.lhsPlan))
          if retrieved is not None:
            if not retrieved.root.operatorType().endswith("Join"):
              self.joinList.append(retrieved.root)
          if currNode is prevNode:
            return None
          else:
            prevNode.subplan = None
            return preJoin
        elif currType is "Project" or currType == "Select" or currType == "GroupBy":
          prevNode = currNode
          if currNode.subPlan is None:
            currNode = None
          else:
            currNode = currNode.subPlan

        elif currType is "Union":
          prevNode = currNode
          if currNode.rhsPlan is None:
            currNode = None
          else:
            currNode = currNode.rhsPlan

        elif currType is "TableScan":
          return preJoin

    return preJoin
