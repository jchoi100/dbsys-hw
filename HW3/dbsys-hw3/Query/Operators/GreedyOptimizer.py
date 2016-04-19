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

class GreedyOptimizer(BushyOptimizer):
  def __init__(self, db):
    super().__init__(db)
    self.db = db
    self.plansConsidered = 0

  def pickJoinOrder(self, plan):
    plan.prepare(self.db)
    # instead of saving an optimalList, just find the most optimalList
    # plan for every length of
