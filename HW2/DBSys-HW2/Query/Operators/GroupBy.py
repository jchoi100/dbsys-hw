from Catalog.Schema import DBSchema
from Query.Operator import Operator

class GroupBy(Operator):
  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined group-by-aggregate operator not supported")

    self.subPlan     = subPlan
    self.subSchema   = subPlan.schema()
    self.groupSchema = kwargs.get("groupSchema", None)
    self.aggSchema   = kwargs.get("aggSchema", None)
    self.groupExpr   = kwargs.get("groupExpr", None)
    self.aggExprs    = kwargs.get("aggExprs", None)
    self.groupHashFn = kwargs.get("groupHashFn", None)

    self.validateGroupBy()
    self.initializeSchema()

  # Perform some basic checking on the group-by operator's parameters.
  def validateGroupBy(self):
    requireAllValid = [self.subPlan, \
                       self.groupSchema, self.aggSchema, \
                       self.groupExpr, self.aggExprs, self.groupHashFn ]

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete group-by specification, missing a required parameter")

    if not self.aggExprs:
      raise ValueError("Group-by needs at least one aggregate expression")

    if len(self.aggExprs) != len(self.aggSchema.fields):
      raise ValueError("Invalid aggregate fields: schema mismatch")

  # Initializes the group-by's schema as a concatenation of the group-by
  # fields and all aggregate fields.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    self.outputSchema = DBSchema(schema, fields)

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "GroupBy"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for selection operator.
  def __iter__(self):
    self.initializeOutput()
    self.outputIterator = self.processAllPages()
    return self

  def __next__(self):
    return next(self.outputIterator)

  def tupleInstanceTest(self, groupExpr):
    if not isinstance(groupExpr, tuple):
      return (groupExpr,)
    else:
      return groupExpr

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  def createPartition(self, pri, groupKey):
    self.storage.createRelation(pri, self.subSchema)
    self.partitionFiles[groupKey] = pri    

  # Set-at-a-time operator processing
  def processAllPages(self):
    self.partitionFiles = {}

    # Partition the inputs using hash on the GroupBy attributes
    for (pageId, page) in iter(self.subPlan):
      for eachTuple in page:
        groupByValue = self.tupleInstanceTest(self.groupExpr(self.subSchema.unpack(eachTuple)))
        groupKey = self.groupHashFn(groupByValue)

        # Generate a partition relation identifier for the partition
        pri = self.operatorType() + str(self.id()) + "PartitionKey_" + str(groupKey)

        # if the partition doesn't already exist, make one.
        if not self.storage.hasRelation(pri):
          self.createPartition(pri, groupKey)

        # fetch the created/already-existing partition file
        partitionFile = self.storage.fileMgr.relationFile(pri)[1]

        # just check once more that it's not null
        if partitionFile:
          # put the tuple inside the partition
          partitionFile.insertTuple(eachTuple)

    # Process each partition file by using their identifiers to fetch them.
    # Then do the aggregation operation.
    for pri in self.partitionFiles.values():
      #Fetch the partition file
      partitionFile = self.storage.fileMgr.relationFile(pri)[1]

      # Let's accumulate all the aggreates using zip() and filters
      # we iterate through each page in the partition file
      accumulation = {}
      # we need to iterate through each page and each tuple in each page for this
      for (pageId, page) in partitionFile.pages():
        for eachTuple in page:
          # the current tuple we're looking at--unpack it
          currTuple = self.subSchema.unpack(eachTuple)
          groupKey = self.tupleInstanceTest(self.groupExpr(currTuple))

          # If we're dealing with this group for the first time...
          if groupKey not in accumulation:
            # put it in
            accumulation[groupKey] = [x[0] for x in self.aggExprs]

          # Update the accumulation value at this group key using zipped tuples and
          # a lambda function.
          zipped = zip([x[1] for x in self.aggExprs], accumulation[groupKey])
          accumulation[groupKey] = list(map(lambda x: x[0](x[1], currTuple), zipped))
      
      # Finalize and output the tuples for this partition.
      for (groupKey, aggregateValue) in accumulation.items():
        zipped = zip([x[2] for x in self.aggExprs], aggregateValue)
        valueToFinalize = list(map(lambda x: x[0](x[1]), zipped))
        tutpleToOutput = self.outputSchema.instantiate(*(list(groupKey) + valueToFinalize))
        self.emitOutputTuple(self.outputSchema.pack(tutpleToOutput))

      if self.outputPages:
        self.outputPages = [self.outputPages[-1]]

    #need to delete and clear up the partition files?

    return self.storage.pages(self.relationId())
          
  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"
