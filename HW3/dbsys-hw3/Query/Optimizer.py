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

class Optimizer:
  """
  A query optimization class.

  This implements System-R style query optimization, using dynamic programming.
  We only consider left-deep plan trees here.

  We provide doctests for example usage only.
  Implementations and cost heuristics may vary.

  >>> import Database
  >>> db = Database.Database()
  >>> try:
  ...   db.createRelation('department', [('did', 'int'), ('eid', 'int')])
  ...   db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  ... except ValueError:
  ...   pass

  # Join Order Optimization
  >>> query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid').finalize()

  >>> db.optimizer.pickJoinOrder(query4)

  # Pushdown Optimization
  >>> query5 = db.query().fromTable('employee').union(db.query().\
        fromTable('employee')).join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid')\
        .where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
        .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()

  >>> db.optimizer.pushdownOperators(query5)

  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}
    self.rawPredicates = [] # list of CNF-formatted selectExprs
    self.predicates = [] # list of CNF-decomposed selectExprs
    self.rawProjPredicates = [] # list of comma-formatted projectExprs
    self.projPredicates = [] # list of comma-separated projectExprs

    self.joinList = list() #Tables (possibly with non-join operations) that are joined in the initial query

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    if plan not in self.statsCache:
      if "Join" in plan.root.operatorType():
        cacheKey = (plan, plan.root.joinMethod, tuple(plan.root.inputs()))
    else:
      cacheKey = (plan, None, tuple(plan.root.inputs()))
      self.statsCache[key] = cost

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    if "Join" in plan.root.operatorType():
      cacheKey = (plan, plan.root.joinMethod, tuple(plan.root.inputs()))
    else:
      cacheKey = (plan, None, tuple(plan.root.inputs()))

    if cacheKey not in self.statsCache:
      for i, operator in plan.flatten():
        operator.initializeStatistics()
      plan.sampleCardinality = 0
      plan.prepare(self.db)
      plan.sample(10.0)
      cost = plan.cost(True)
      self.addPlanCost(plan, cost)

    return self.statsCache[cacheKey]

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    plan.prepare(self.db)
    relationsInvolved = plan.relations()
    myRoot = plan.root

    """
    Select pushdown.
    """

    while myRoot.operatorType() is "Select":
      self.rawPredicates.append(myRoot.selectExpr)
      myRoot = myRoot.subPlan

    myRoot = self.traverseTreeSelect(myRoot)

    for rawPredicate in self.rawPredicates:
      decomposedPreds = ExpressionInfo(rawPredicate).decomposeCNF()
      for decomposedPred in decomposedPreds:
        self.predicates.append(decomposedPred)

    # Reattach select predicates
    for predicate in self.predicates:
      predAttributes = ExpressionInfo(predicate).getAttributes()
      # Traverse the tree looking for any operator that contains
      # all of the attributes in predAttributes.
      # If the currOperator has all of them, check if currOperator's
      # subPlan (or lhsPlan or rhsPlan depending on operatorType())
      # also contains all of them. If so, go down deeper.
      parentPlan = None
      currPlan = myRoot
      if currPlan.operatorType() is "GroupBy":
        parentPlan = currPlan
        currPlan = currPlan.subPlan

      isLeftChild = False
      currPlan, backupParent = self.findFirstMatch(currPlan, parentPlan, predAttributes)
      currPAttributes = currPlan.schema().fields

      while self.firstIsSubsetOfSecond(predAttributes, currPAttributes):
        if currPlan.operatorType().endswith("Join") or \
           currPlan.operatorType() is "Union":
          leftChild = currPlan.lhsPlan
          leftAttributes = leftChild.schema().fields
          rightChild = currPlan.rhsPlan
          rightAttributes = rightChild.schema().fields

          if self.firstIsSubsetOfSecond(predAttributes, leftAttributes):
            parentPlan = currPlan
            currPlan = leftChild
            currPAttributes = currPlan.schema().fields
            isLeftChild = True
          elif self.firstIsSubsetOfSecond(predAttributes, rightAttributes):
            parentPlan = currPlan
            currPlan = rightChild
            currPAttributes = currPlan.schema().fields
            isLeftChild = False
          else:
            break
        elif currPlan.operatorType() is "Project":
          onlyChild = currPlan.subPlan
          childAttributes = onlyChild.schema().fields
          if self.firstIsSubsetOfSecond(predAttributes, childAttributes):
            parentPlan = currPlan
            currPlan = onlyChild
            currPAttributes = currPlan.schema().fields
        elif currPlan.operatorType() is "TableScan":
          break
        else:
          parentPlan = currPlan
          currPlan = currPlan.subPlan
          currPAttributes = currPlan.schema().fields
      # Now, we know that the select statement should go between
      # the parentOperator and currOperator.
      selectToAdd = Select(subPlan = currPlan, selectExpr = predicate)

      if not parentPlan:
        if backupParent:
          parentPlan = backupParent
        else:
          selectToAdd.subPlan = currPlan
          myRoot = selectToAdd

      if parentPlan:
        if parentPlan.operatorType().endswith("Join") or \
             parentPlan.operatorType() is "Union":
          if isLeftChild:
            selectToAdd.subPlan = parentPlan.lhsPlan
            parentPlan.lhsPlan = selectToAdd
          else:
            selectToAdd.subPlan = parentPlan.rhsPlan
            parentPlan.rhsPlan = selectToAdd
        else:
          selectToAdd.subPlan = parentPlan.subPlan
          parentPlan.subPlan = selectToAdd

    """
    Project pushdown.
    """
    self.traverseTreeProject(myRoot)
    print(self.projPredicates)

    return Plan(root = myRoot)

  def findFirstMatch(self, currPlan, backupParent, predAttributes):
    currPAttributes = currPlan.schema().fields
    if self.firstIsSubsetOfSecond(predAttributes, currPAttributes):
      return currPlan, backupParent
    else:
      if currPlan.operatorType() is "Select" or \
         currPlan.operatorType() is "GroupBy" or \
         currPlan.operatorType() is "Project":
        return self.findFirstMatch(currPlan.subPlan, currPlan, predAttributes)
      elif currPlan.operatorType() is "TableScan":
        return None
      elif currPlan.operatorType() is "Union" or \
            currPlan.operatorType().endswith("Join"):
        leftSearch, bup = self.findFirstMatch(currPlan.lhsPlan, currPlan, predAttributes)
        rightSearch, bup = self.findFirstMatch(currPlan.rhsPlan, currPlan, predAttributes)
        if leftSearch is not None:
          return leftSearch, bup
        else:
          return rightSearch, bup

  # Traverse the plan tree while picking out the select operators.
  # Save the picked out select operators in self.rawPredicates.
  # Recursively traverse the tree. Upon returning from the previous
  # recursion, save the result from the previous recursive call
  # to curr's subPlan (or lhsPlan or rhsPlan depending on optype).
  # @param curr: currNode
  def traverseTreeSelect(self, curr):
    if curr.operatorType().endswith("Join") or \
      curr.operatorType() is "Union":
      leftChild = curr.lhsPlan
      rightChild = curr.rhsPlan
      while leftChild.operatorType() is "Select":
        self.rawPredicates.append(leftChild.selectExpr)
        curr.lhsPlan = leftChild.subPlan
        leftChild = curr.lhsPlan
      while rightChild.operatorType() is "Select":
        self.rawPredicates.append(rightChild.selectExpr)
        curr.rhsPlan = rightChild.subPlan
        rightChild = curr.rhsPlan
      curr.lhsPlan = self.traverseTreeSelect(leftChild)
      curr.rhsPlan = self.traverseTreeSelect(rightChild)
    elif curr.operatorType() is "Project":
      childPlan = curr.subPlan
      while childPlan.operatorType() is "Select":
        self.rawPredicates.append(childPlan.selectExpr)
        curr.subPlan = childPlan.subPlan
        childPlan = curr.subPlan
      curr.subPlan = self.traverseTreeSelect(childPlan)
    elif curr.operatorType() is "Select":
      self.rawPredicates.append(curr.selectExpr)
      curr.subPlan = self.traverseTreeSelect(curr.subPlan)
    elif curr.operatorType() is "TableScan":
      return curr
    else:
      childPlan = curr.subPlan
      while childPlan.operatorType() is "Select":
        self.rawPredicates.append(childPlan.selectExpr)
        curr.subPlan = childPlan.subPlan
        childPlan = curr.subPlan
      curr.subPlan = self.traverseTreeSelect(childPlan)
    return curr

  def traverseTreeProject(self, curr):
    if curr.operatorType().endswith("Join"):
      currRawJoinExpr = curr.JoinExpr
      currJoinExprs = ExpressionInfo(currRawJoinExpr).decomposeCNF()
      print("===================================")
      print("currJoinExprs:")
      print(currJoinExprs)
      for currJoinExpr in currJoinExprs:
        splitExprs = currJoinExpr.split("==")
        for splitExpr in splitExprs:
          self.projPredicates.append(splitExpr)
      self.traverseTreeProject(curr.lhsPlan)
      self.traverseTreeProject(curr.rhsPlan)
    elif curr.operatorType() is "GroupBy":
      gbSchemaFields = curr.groupSchema.fields
      print("===================================")
      print("gbSchemaFields:")
      print(gbSchemaFields)
      for field in gbSchemaFields:
        self.projPredicates.append(field)
      self.traverseTreeProject(curr.subPlan)
    elif curr.operatorType() is "Project":
      print("===================================")
      print("rawProjPreds:")
      rawProjPreds = curr.projectExprs
      print(rawProjPreds)
    elif curr.operatorType() is "Select":
      rawSelectPreds = curr.selectExpr
      selExprs = ExpressionInfo(rawSelectPreds).decomposeCNF()
      print("===================================")
      print("selExprs:")
      print(selExprs)
      for selExpr in selExprs:
        self.projPredicates.append(selExpr)
      self.traverseTreeProject(curr.subPlan)
    elif curr.operatorType() is "Union":
      fields = curr.schema().fields
      print("===================================")
      print("union fields:")
      print(fields)
      for field in fields:
        self.projPredicates.append(field)
      self.traverseTreeProject(curr.subPlan)
    else:
      print("Out!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      return

  # def traverseTreeProject(self, curr):
  #   if curr.operatorType().endswith("Join") or \
  #     curr.operatorType() is "Union":
  #     leftChild = curr.lhsPlan
  #     rightChild = curr.rhsPlan
  #     while leftChild.operatorType() is "Project":
  #       self.rawProjPredicates.append(leftChild.projectExprs)
  #       leftChild = leftChild.subPlan
  #     while rightChild.operatorType() is "Project":
  #       self.rawProjPredicates.append(rightChild.projectExprs)
  #       rightChild = rightChild.subPlan
  #     curr.lhsPlan = self.traverseTreeProject(leftChild)
  #     curr.rhsPlan = self.traverseTreeProject(rightChild)
  #   elif curr.operatorType() is "Select":
  #     childPlan = curr.subPlan
  #     while childPlan.operatorType() is "Project":
  #       self.rawProjPredicates.append(childPlan.projectExprs)
  #       childPlan = childPlan.subPlan
  #     curr.subPlan = self.traverseTreeProject(childPlan)
  #   elif curr.operatorType() is "Project":
  #     self.rawProjPredicates.append(curr.projectExprs)
  #     curr.subPlan = self.traverseTreeProject(curr.subPlan)
  #   elif curr.operatorType() is "TableScan":
  #     return curr
  #   else:
  #     childPlan = curr.subPlan
  #     while childPlan.operatorType() is "Project":
  #       self.rawProjPredicates.append(childPlan.projectExprs)
  #       childPlan = childPlan.subPlan
  #     curr.subPlan = self.traverseTreeProject(childPlan)
  #   return curr

  # Check if "firstSet" is a subset of "secondSet".
  # Both input "set"s are python lists.
  # Return True if so, False if not.
  def firstIsSubsetOfSecond(self, firstSet, secondSet):
    for elem in firstSet:
      if elem not in secondSet:
        return False
    return True

  # # Returns an optimized query plan with joins ordered via a System-R style
  # # dyanmic programming algorithm. The plan cost should be compared with the
  # # use of the cost model below.
  # def pickJoinOrder(self, plan):
  #   plan.prepare(self.db)

  #   preJoin = self.getJoins(plan)


  #   # For each pass, do:
  #   # 1) an enumeration of viable candidate plans for the given subsets of relations
  #   # 2) and an evaluation of the best plan in each subset


  #   #for i in len(relationsInvolved):
  #     #joinList.append(Plan(root=relationsInvolved[i]))

  #   for i in len(relationsInvolved):
  #     newList = list()
  #     for j in joinList:
  #       for k in relationsInvolved:
  #         if k in j.relations():
  #           #Test the different join orderings
  #           oldFirst =  True
  #           joinType = "block-nested-loops"
  #           minCost = 2147483647

  #           #Test the 2 orders and 2 kinds of join's costs. Return with the minimum
  #           tempPlan = Plan()

  #           cost = self.getPlanCost(tempPlan)

  #           newList.append(tempPlan)

  #     joinList = newList

  #   return Plan(root = newList[0])

  # def getJoins(self, plan):
  #   #Everything up to the first join.


  #   #preJoin = plan
  #   preJoin = copy.copy(plan)
  #   foundJoin = False

  #   currNode = plan.root
  #   prevNode = plan.root

  #   while currNode is not None:
  #     currType = currNode.operatorType()
  #     prevType = prevNode.operatorType()

  #     if foundJoin is False:
  #       if "Join" in currType:
  #         foundJoin = True

  #         if prevType is "Project" or prevType is "Select" or prevType is "TableScan" or prevType is "GroupBy":
  #           prevNode.subPlan = None

  #         elif prevType is "Union":
  #           prevNode.lhsPlan = None

  #       elif currType is "Project" or currType is "Select" or currType is "TableScan" or currType is "GroupBy":
  #         prevNode = currNode
  #         if currNode.subPlan is None:
  #           currNode = None
  #         else:
  #           currNode = currNode.subPlan

  #       elif currType is "Union":
  #         prevNode = currNodee
  #         if currNode.lhsPlan is None:
  #           currNode = None
  #         else:
  #           currNode = currNode.lhsPlan

  #     elif foundJoin is True:
  #       if "Join" in currType:
  #         self.joinList.append(currNode.rhsPlan)
  #         self.joinList.append(self.getJoins(Plan(currNode.lhsPlan))

  #       elif currType is "Project" or currType == "Select" or currType == "TableScan" or currType == "GroupBy":
  #         prevNode = currNode
  #         if currNode.subPlan is None:
  #           currNode = None
  #         else:
  #           currNode = currNode.subPlan

  #       elif currType is "Union":
  #         prevNode = currNode
  #         if currNode.lhsPlan is None:
  #           currNode = None
  #         else:
  #           currNode = currNode.lhsPlan

  #   return preJoin

  def swapJoinSubPlans(self, join):
    temp = join.lhsPlan
    join.lhsPlan = join.rhsPlan
    join.rhsPlan = temp
    join.lhsSchema = join.lhsPlan.schema()
    join.rhsSchema = join.rhsPlan.schema()
    join.initializeSchema()
    return join


  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)
    return joinPicked_plan

if __name__ == "__main__":
  import doctest
  doctest.testmod()
