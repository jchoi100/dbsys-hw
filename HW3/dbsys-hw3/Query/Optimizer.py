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

  def traverseTree(self, curr, all_selects):

    if curr.operatorType().endswith("Join") or\
       curr.operatorType() is "Union":
      leftChild = curr.lhsPlan
      rightChild = curr.rhsPlan

      while leftChild.operatorType is "Select":
        all_selects.append(leftChild.selectExpr)
        curr.lhsPlan = leftChild.subPlan
        leftChild = curr.lhsPlan

      while rightChild.operatorType() is "Select":
        all_selects.append(rightChild.selectExpr)
        curr.rhsPlan = rightChild.subPlan
        rightChild = curr.rhsPlan

      traverseTree(curr.lhsPlan, all_selects)
      traverseTree(curr.rhsPlan, all_selects)

    else:
      traverseTree(curr.subPlan, all_selects)

    return curr, all_selects


  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    plan.prepare(self.db)
    root = plan.root
    all_relations_list = plan.relations()
    all_selects = []

    while root.operatorType() is "Select":
      all_selects.append(root.selectExpr)
      root = root.subPlan

    # new_root: root of new tree with all the selects removed.
    # all_selects: list of all the select exprs from orig tree.
    new_root, all_selects = traverseTree(root, all_selects)

    newPlan = Plan(root = new_root)

    # To be used to find out parent links.
    allPlans = newPlan.flatten()

    # Place all cnf-decomposed expressions in the following list.
    cnf_decomposed_selects = []
    for predicate in all_selects:
      decomposed = ExpressionInfo(predicate).decomposeCNF()
      for expr in decomposed:
        cnf_decomposed_selects.append(expr)

    # Traverse through each select statement and see where they
    # can go. Start from the bottom of the tree for each expr.

    attribute_dict = {}
    for relation in all_relations_list:
      att_list = relation.schema().fields
      for att in att_list:
        attribute_dict[att] = relation

    for predicate in cnf_decomposed_selects:


"""
"""

  def firstSubsetOfSecond(self, firstSet, secondSet):
    for elem in firstSet:
      if elem not in secondSet:
        return False
    return True

  def pushDownOperatorsObsolete(self, plan):
    allPlans = plan.flatten()
    toBePushedDown = deque()

    for i in range(len(allPlans)):
      root = plan.root
      toBePushedDown.append(root)

      while toBePushedDown:
        currDepth, currOperator = toBePushedDown.popleft()
        currOperatorType = currOperator.operatorType()

        if currOperatorType is "Project" or currOperatorType is "Select":
          if (currDepth + 1, currOperator.subPlan) in allPlans:
            subPosition = allPlans.index((currDepth +  1, currOperator.subPlan))
            subDepth, subOperator = allPlans[subPosition]
            flag = False
            subOperatorType = subOperator.operatorType()
            # Case when child operator is a Project or Select
            if subOperatorType is "Project" or subOperatorType is "Select":
              # Push down currOperator to become subOperator's subplan
              currOperator.subPlan = subOperator.subPlan
              subOperator.subPlan = currOperator

              currParentDepth = currDepth - 1
              for depth, currParent in allPlans:
                if depth == currParentDepth and currParent.subPlan is currOperator:
                  currParent.subPlan = subOperator
              toBePushedDown.extendleft((currDepth + 1, currOperator))
            elif subOperatorType.endswith("Join") or subOperatorType is "Union":
              lFields = subOperator.lhsPlan.schema().fields
              rFields = subOperator.rhsPlan.schema().fields
              currOperator.inputSchemas()[0].fields

              if currOperatorType is "Project":
                currExpr = currOperator.projectExprs
                lExpr = {}
                rExpr = {}

                for field, value in currExpr.items():
                  if field in lFields:
                    lExpr[field] = value
                  elif field in rFields:
                    rExpr[field] = value
                lProject = Project(subPlan = subOperator.lhsPlan, \
                                   projectExprs = lExpr)
                rProject = Project(subPlan = subOperator.rhsPlan, \
                                   projectExprs = rExpr)
                subOperator.lhsPlan = lProject
                subOperator.rhsPlan = rProject

                currParentDepth = currDepth - 1
                for depth, currParent in allPlans:
                  if depth == currParentDepth and\
                     currParent.subPlan is currOperator:
                    currParent.subPlan = subOperator
                lProject = Project(subPlan = subOperator.lhsPlan, \
                                   projectExprs = lExpr)
                rProject = Project(subPlan = subOperator.rhsPlan, \
                                   projectExprs = rExpr)

                toBePushedDown.extendleft((currDepth + 1, lProject))
                toBePushedDown.extendleft((currDepth + 1, rProject))

              elif currOperatorType is "Select":
                currExpr = currOperatorType.selectExpr
                attributes = ExpressionInfo(currExpr).getAttributes()
                exprs = ExpressionInfo(currExpr).decomposeCNF()

                lExpr = ""
                rExpr = ""
                sExpr = ""
                lAttributes = set()
                rAttributes = set()

                for attribute in attributes:
                  if attribute in lFields:
                    lAttributes.add(attribute)
                  elif attribute in rFields:
                    rAttributes.add(attribute)

                for expr in exprs:
                  exprAttributes = ExpressionInfo(exp).getAttributes()
                  if exprAttributes.issubset(lAttributes):
                    lExpr += (expr + " and ")
                  elif exprAttributes.issubset(rAttributes):
                    rExpr += (expr + " and ")
                  else:
                    sExpr += (expr + " and ")

                # Slice off trailing " and " before.
                if len(lExpr) > 1:
                  if len(lExpr) > 5:
                    lExpr = lExpr[0 : len(lExpr) - 5]
                  lSelect = Select(subPlan = subOperator.lhsPlan, \
                                   selectExpr = lExpr)
                  subOperator.lhsPlan = lSelect

                if len(rExpr) > 1:
                  if len(rExpr) > 5:
                    rExpr = rExpr[0 : len(rExpr) - 5]
                  rSelect = Select(subPlan = subOperator.rhsPlan, \
                                   selectExpr = rExpr)
                  subOperator.rhsPlan = rSelect

                if len(sExpr) > 1:
                  if len(sExpr) > 5:
                    sExpr = sExpr[0 : len(sExpr) - 5]
                  flag = True
                  sSelect = Select(subPlan = subOperator, selectExpr = sExpr)

                currParentDepth = currDepth - 1
                for depth, currParent in allPlans:
                  if depth == currParentDepth and currParent.subPlan is currOperator:
                    if not flag:
                      currParent.subPlan = subOperator
                    else:
                      currParent.subPlan = sSelect
                toBePushedDown.extendleft((currDepth + 1, lSelect))
                toBePushedDown.extendleft((currDepth + 1, rSelect))

            if currDepth == 0:
              if not flag:
                allPlans = Plan(root = subOperator).flatten()
              else:
                allPlans = Plan(root = flagSelect).flatten()
            else:
              (rootDepth, rootOperator) = root
              allPlans = Plan(root = rooftOperator).flatten()
            root = allPlans[0]

    return Plan(root = allPlans[0])

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
