import itertools
import pdb
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

    while root.operatorType() is "Select":
      self.rawPredicates.append(root.selectExpr)
      myRoot = myRoot.subPlan.root

    myRoot = self.traverseTree(myRoot)
    newPlan = Plan(root = myRoot)

    for rawPredicate in rawPredicates:
      decomposedPreds = ExpressionInfo(rawPredicate).decomposeCNF()
      for decomposedPred in decomposedPreds:
        predicates.append(decomposedPred)

    for predicate in predicates:
      predAttributes = ExpressionInfo(predicate).getAttributes()
      # Traverse the tree looking for any operator that contains
      # all of the attributes in predAttributes.
      # If the currOperator has all of them, check if currOperator's
      # subPlan (or lhsPlan or rhsPlan depending on operatorType())
      # also contains all of them. If so, go down deeper.
      parentPlan = None
      currPlan = newPlan
      currPAttributes = currPlan.root.schema().fields
      isLeftChild = False
      while self.firstIsSubsetOfSecond(predAttributes, currPAttributes):
        if currPlan.root.operatorType().endswith("Join") or \
           currPlan.root.operatorType() is "Union":
          leftChild = currPlan.root.lhsPlan
          leftAttributes = leftChild.root.schema().fields
          rightChild = currPlan.root.rhsPlan
          rightAttributes = rightChild.root.schema().fields

          if self.firstIsSubsetOfSecond(predAttributes, leftAttributes):
            parentPlan = currPlan
            currPlan = leftChild
            currPAttributes = currPlan.root.schema().fields
            isLeftChild = True
          elif self.firstIsSubsetOfSecond(predAttributes, rightAttributes):
            parentPlan = currPlan
            currPlan = rightChild
            currPAttributes = currPlan.root.schema().fields
            isLeftChild = False
          else:
            break
        elif currPlan.root.operatorType() is "Project":
          onlyChild = currOperator.subPlan
          childAttributes = onlyChild.root.schema().fields
          if self.firstIsSubsetOfSecond(predAttributes, childAttributes):
            parentPlan = currPlan
            currPlan = onlyChild
            currPAttributes = currPlan.root.schema().fields
        elif currPlan.root.operatorType() is "TableScan":
          break
        else:
          currPlan = currPlan.subPlan
          currPAttributes = currPlan.root.schema().fields
      # Now, we know that the select statement should go between
      # the parentOperator and currOperator.
      selectToAdd = Select(subPlan = currPlan, selectExpr = predicate)
      if parentPlan.operatorType()endswith("Join") or \
           parentPlan.operatorType() is "Union":
        if isLeftChild:
          parentPlan.lhsPlan = selectToAdd
        else:
          parentPlan.rhsPlan = selectToAdd
      else:
        parentPlan.subPlan = selectToAdd
    return newPlan

  # Traverse the plan tree while picking out the select operators.
  # Save the picked out select operators in self.rawPredicates.
  # Recursively traverse the tree. Upon returning from the previous
  # recursion, save the result from the previous recursive call
  # to curr's subPlan (or lhsPlan or rhsPlan depending on optype).
  # @param curr: currNode
  def traverseTree(self, curr):
    if curr.operatorType().endswith("Join") or \
      curr.operatorType() is "Union":
      leftChild = curr.lhsPlan
      rightChild = curr.rhsPlan
      while leftChild.root.operatorType() is "Select":
        rawPredicates.append(leftChild.root.selectExpr)
        curr.lhsPlan = leftChild.subPlan
        leftChild = curr.lhsPlan
      while rightChild.root.operatorType() is "Select":
        rawPredicates.append(rightChild.root.selectExpr)
        curr.rhsPlan = rightChild.subPlan
        rightChild = curr.rhsPlan
      curr.lhsPlan = self.traverseTree(leftChild.root)
      curr.rhsPlan = self.traverseTree(rightChild.root)
    elif curr.operatorType() is "Project":
      childPlan = curr.subPlan
      while childPlan.root.operatorType() is "Select":
        rawPredicates.append(childPlan.root.selectExpr)
        curr.subPlan = childPlan.subPlan
        childPlan = curr.subPlan
      curr.subPlan = self.traverseTree(childPlan.root)
    else:
      curr.subPlan = self.traverseTree(curr.subPlan)
    return curr

  # Check if "firstSet" is a subset of "secondSet".
  # Both input "set"s are python lists.
  # Return True if so, False if not.
  def firstIsSubsetOfSecond(self, firstSet, secondSet):
    for elem in firstSet:
      if elem not in secondSet:
        return False
    return True

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    # Retrieve all the subplans in this plan
    allPlans = plan.flatten()

    # For each pass, do:
    # 1) an enumeration of viable candidate plans for the given subsets of relations
    # 2) and an evaluation of the best plan in each subset

    for i in range(len(allPlans)):
      root = plan.root

    cost = self.getPlanCost(plan)

    return Plan(root = allPlans[0])

  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)
    return joinPicked_plan

if __name__ == "__main__":
  import doctest
  doctest.testmod()
