from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Union(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)
    self.lhsPlan  = lhsPlan
    self.rhsPlan  = rhsPlan
    self.validateSchema()

  # Unions must have equivalent schemas on their left and right inputs.
  def validateSchema(self):
    if self.lhsPlan.schema().match(self.rhsPlan.schema()):
      schemaName       = self.operatorType() + str(self.id())
      schemaFields     = self.lhsPlan.schema().schema()
      self.unionSchema = DBSchema(schemaName, schemaFields)
    else:
      raise ValueError("Union operator type error, mismatched input schemas")

  def schema(self):
    return self.unionSchema

  def inputSchemas(self):
    return [self.lhsPlan.schema(), self.rhsPlan.schema()]

  def operatorType(self):
    return "UnionAll"

  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for union operator.
  # The iterator must be set up to deal with input iterators and handle both pipelined and
  # non-pipelined cases
  # TODO assignment (Exercise 1)
  def __iter__(self):
    # First guess - from what I've seen in Operators.Select.__iter__
    self.initializeOutput()
    self.rightIterator = iter(self.rhsPlan)
    self.leftIterator = iter(self.lhsPlan)
    self.inputFinished = False

    if not self.pipelined:
      self.outputIterator = self.processAllPages()

    return self

  # Method used for iteration, doing work in the process. Handle pipelined and non-pipelined cases
  # TODO assignment (Exercise 1)
  def __next__(self):
    if self.pipelined:
      while not(self.inputFinished or self.isOutputPageReady()):
        try:
          pageId, page = next(self.inputIterator)
          self.processInputPage(pageId, page)
        except StopIteration:
          self.inputFinished = True

      return self.outputPage()

    else:
      return next(self.outputIterator)



  # Page processing and control methods

  # Page-at-a-time operator processing
  # For union all, this copies over the input tuple to the output
  # TODO assignment (Exercise 1)
  def processInputPage(self, pageId, page):
    # Both plans have the same schema after the validateSchema
    # schema = self.lhsPlan.schema()
    schema = self.unionSchema

    if set(locals().keys()).isdisjoint(set(schema.fields)):
      for inputTuple in page:
        # Load tuple fields into the select expression context
        selectExprEnv = self.loadSchema(schema, inputTuple)

        # Union
        self.emitOutputTuple(inputTuple)
    else:
      raise ValueError("Overlapping variables detected with operator schema")

  # Set-at-a-time operator processing
  # TODO assignment (Exercise 1)
  def processAllPages(self):
    inputIterator = iter(self.lhsPlan)

    try:
      # Process all pages from the left plan
      for (pageId, page) in inputIterator:
        self.processInputPage(pageId, page)

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

      # Process all pages from the right plan
      inputIterator = iter(self.rhsPlan)
      for (pageId, page) in inputIterator:
        self.processInputPage(pageId, page)

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

    # To support pipelined operation, processInputPage may raise a
    # StopIteration exception during its work. We catch this and ignore in batch mode.
    except StopIteration:
      pass

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())
