from Catalog.Schema import DBSchema
from Query.Operator import Operator

# Operator for External Sort
class Sort(Operator):

  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)
    self.subPlan     = subPlan
    self.sortKeyFn   = kwargs.get("sortKeyFn", None)
    self.sortKeyDesc = kwargs.get("sortKeyDesc", None)

    if self.sortKeyFn is None or self.sortKeyDesc is None:
      raise ValueError("No sort key extractor provided to a sort operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.subPlan.schema()

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "Sort"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]


  # Iterator abstraction for external sort operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputIterator = iter(self.subPlan)
    self.inputFinished = False
    if not self.pipelined:
      self.outputIterator = self.processAllPages()
    return self

  def __next__(self):
    if not self.pipelined:
      return next(self.outputIterator)
    else:
      while not(self.inputFinished or self.isOutputPageReady()):
        try:
          pageId, page = next(self.inputIterator)
          self.processInputPage(pageId, page)
        except StopIteration:
          self.inputFinished = True
      return self.outputPage()

  # Page processing and control methods
  #Psuedocode from Josh:
    #The algorithm should repeatedly:
      #load a single 'block' of input
      #sort it (in-place for extra credit, otherwise, you can materialize the tuples as Python objects and sort them in-memory)
      #write the result out to a temporary file

    #This will leave you with multiple sorted files, which you will need to merge into a single sorted output file.

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise NotImplementedError

  # Set-at-a-time operator processing
  def processAllPages(self):
    raise NotImplementedError

  def cost(self):
    return self.selectivity() * self.subPlan.cost()

  def selectivity(self):
    return 1.0
