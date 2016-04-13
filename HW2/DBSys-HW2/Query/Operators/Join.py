import itertools

from Catalog.Schema import DBSchema
from Query.Operator import Operator

class Join(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined join operator not supported")

    self.lhsPlan    = lhsPlan
    self.rhsPlan    = rhsPlan
    self.joinExpr   = kwargs.get("expr", None)
    self.joinMethod = kwargs.get("method", None)
    self.lhsSchema  = kwargs.get("lhsSchema", None if lhsPlan is None else lhsPlan.schema())
    self.rhsSchema  = kwargs.get("rhsSchema", None if rhsPlan is None else rhsPlan.schema())

    self.lhsKeySchema   = kwargs.get("lhsKeySchema", None)
    self.rhsKeySchema   = kwargs.get("rhsKeySchema", None)
    self.lhsHashFn      = kwargs.get("lhsHashFn", None)
    self.rhsHashFn      = kwargs.get("rhsHashFn", None)

    self.validateJoin()
    self.initializeSchema()
    self.initializeMethod(**kwargs)

  # Checks the join parameters.
  def validateJoin(self):
    # Valid join methods: "nested-loops", "block-nested-loops", "indexed", "hash"
    if self.joinMethod not in ["nested-loops", "block-nested-loops", "indexed", "hash"]:
      raise ValueError("Invalid join method in join operator")

    # Check all fields are valid.
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      methodParams = [self.joinExpr]

    elif self.joinMethod == "indexed":
      methodParams = [self.lhsKeySchema]

    elif self.joinMethod == "hash":
      methodParams = [self.lhsHashFn, self.lhsKeySchema, \
                      self.rhsHashFn, self.rhsKeySchema]

    requireAllValid = [self.lhsPlan, self.rhsPlan, \
                       self.joinMethod, \
                       self.lhsSchema, self.rhsSchema ] \
                       + methodParams

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete join specification, missing join operator parameter")

    # For now, we assume that the LHS and RHS schema have
    # disjoint attribute names, enforcing this here.
    for lhsAttr in self.lhsSchema.fields:
      if lhsAttr in self.rhsSchema.fields:
        raise ValueError("Invalid join inputs, overlapping schema detected")


  # Initializes the output schema for this join.
  # This is a concatenation of all fields in the lhs and rhs schema.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.lhsSchema.schema() + self.rhsSchema.schema()
    self.joinSchema = DBSchema(schema, fields)

  # Initializes any additional operator parameters based on the join method.
  def initializeMethod(self, **kwargs):
    if self.joinMethod == "indexed":
      self.indexId = kwargs.get("indexId", None)
      if self.indexId is None or self.lhsKeySchema is None:
        raise ValueError("Invalid index for use in join operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.joinSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.lhsSchema, self.rhsSchema]

  # Returns a string describing the operator type
  def operatorType(self):
    readableJoinTypes = { 'nested-loops'       : 'NL'
                        , 'block-nested-loops' : 'BNL'
                        , 'indexed'            : 'Index'
                        , 'hash'               : 'Hash' }
    return readableJoinTypes[self.joinMethod] + "Join"

  # Returns child operators if present
  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for join operator.
  def __iter__(self):
    self.initializeOutput()
    self.partitionFiles = {0:{}, 1:{}}
    self.outputIterator = self.processAllPages()
    return self

  def __next__(self):
    return next(self.outputIterator)

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    if self.joinMethod == "nested-loops":
      return self.nestedLoops()

    elif self.joinMethod == "block-nested-loops":
      return self.blockNestedLoops()

    elif self.joinMethod == "indexed":
      return self.indexedNestedLoops()

    elif self.joinMethod == "hash":
      return self.hashJoin()

    else:
      raise ValueError("Invalid join method in join operator")


  ##################################
  #
  # Nested loops implementation
  #
  def nestedLoops(self):
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        # Load the lhs once per inner loop.
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

        for (rPageId, rhsPage) in iter(self.rhsPlan):
          for rTuple in rhsPage:
            # Load the RHS tuple fields.
            joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

            # Evaluate the join predicate, and output if we have a match.
            if eval(self.joinExpr, globals(), joinExprEnv):
              outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())


  ##################################
  #
  # Block nested loops implementation
  #
  # This attempts to use all the free pages in the buffer pool
  # for its block of the outer relation.

  # Accesses a block of pages from an iterator.
  # This method pins pages in the buffer pool during its access.
  # We track the page ids in the block to unpin them after processing the block.
  def accessPageBlock(self, bufPool, pageIterator):
    # the page block to be returned
    pageBlock = []
    try:
      while True:
        #get the pageId and page using pageIterator
        (pageId, page) = next(pageIterator)
        pageBlock.append((pageId, page))
        #pin the page
        bufPool.pinPage(pageId)
        if bufPool.numFreePages() == 0:
          break
    except StopIteration:
      pass

    return pageBlock

  # A block nested loop join takes blocks from the left hand side
  # and the right hand side, and for each left block, it iterates
  # through each right block, and for each of the iterations,
  # we check for each tuple in the left block if any match
  # with a tuple in the right block currently in our hands.
  # So naturally, this requires 4 for loops.
  def blockNestedLoops(self):
    # our bufferpool
    bufPool = self.storage.bufferPool

    # iterates through the left relation
    leftIterator = iter(self.lhsPlan)

    # The first block in the left relation
    leftPageBlock = self.accessPageBlock(bufPool, leftIterator)

    # Iterate while there are left blocks remaining
    while leftPageBlock:
      # For each left block
      for (leftPageId, leftPage) in leftPageBlock:
        # And for each tuple in this left block
        for leftTuple in leftPage:
          # load tuple
          joinExpSetup = self.loadSchema(self.lhsSchema, leftTuple)
          # for each right block
          for (rightPageId, rightPage) in iter(self.rhsPlan):
            # for each right block tuple
            for rightTuple in rightPage:
              # load tuple
              joinExpSetup.update(self.loadSchema(self.rhsSchema, rightTuple))
              # evaluate equi-join condition
              if eval(self.joinExpr, globals(), joinExpSetup):
                output = self.joinSchema.instantiate(*[joinExpSetup[f] for f in self.joinSchema.fields])
                self.emitOutputTuple(self.joinSchema.pack(output))
          # track the last output page
          if self.outputPages:
            self.outputPages = [self.outputPages[-1]]
        bufPool.unpinPage(leftPageId)
      #Let's move on to the next left block!
      leftPageBlock = self.accessPageBlock(bufPool, leftIterator)
    # we need to return an iterator to the output
    return self.storage.pages(self.relationId())

  ##################################
  #
  # Indexed nested loops implementation
  #
  # TODO assignment (Exercise 6)
  # Pseudocode - Cow 492
  # Pseudocode from Josh
  # for lTuple in lhs:
  #  joinKey = project lTuple onto lhsKeySchema
    #matches = fileManager.lookupByIndex(rhsRelationId, indexId, joinKey)
    #for rTuple in matches:
    #    fullMatch = evaluate join expression
    #    if fullMatch:
#            materialize and emit an output tuple
  def indexedNestedLoops(self):
    fileManager = self.storage.fileMgr

    #Iterate through left tuples
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:

        joinExpSetup = self.loadSchema(self.lhsSchema, lTuple)

        joinKey = self.lhsSchema.projectBinary(lTuple, self.lhsKeySchema)

        rhsRelationId = self.rhsPlan.relationId()

        matches = fileManager.lookupByIndex(rhsRelationId, self.indexId, joinKey)

        #Iterate through right tuples that matched by index
        if matches:
          for tupleId in matches:
            page = self.storage.bufferPool.getPage(tupleId.pageId)
            tuple = page.getTuple(tupleId)
            joinExpSetup.update(self.loadSchema(self.rhsSchema, tuple))
            if(eval(self.joinExpr, globals(), joinExpSetup)):
              outputTuple = self.joinSchema.instantiate(*[joinExpSetup[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

    return self.storage.pages(self.relationId())

  ##################################
  #
  # Hash join implementation.
  #
  # TODO assignment (Exercise 3)
  # Psuedocode - Boat 559, Cow 499
  def hashJoin(self):
    # May not need this
    if not self.partitionFiles:
      self.partitionFiles = {0:{}, 1:{}}

    self.partitionSide(left=True)
    self.partitionSide(left=False)

    #Create the pair iterator
    leftKeys = self.partitionFiles[0].keys()
    rightKeys = self.partitionFiles[1].keys()

    matches = [(self.partitionFiles[0][partId], self.partitionFiles[1][partId]) \
    for partId in leftKeys if partId in rightKeys]
    pairIterator = PartitionIterator(matches, self.storage)

    #Iterature over partitions and match as neccessary
    for ((leftId, leftPage), (rightId, rightPage)) in pairIterator:
      for leftTuple in leftPage:
        joinExpSetup = self.loadSchema(self.lhsSchema, leftTuple)
        for rightTuple in rightPage:
          joinExpSetup.update(self.loadSchema(self.rhsSchema, rightTuple))

          #Determine if we should output this tuple
          shouldOutput = False
          if self.lhsSchema.projectBinary(leftTuple, self.lhsKeySchema)\
          == self.rhsSchema.projectBinary(rightTuple, self.rhsKeySchema):
            if self.joinExpr:
              shouldOutput = eval(self.joinExpr, globals(), joinExpSetup)
            else:
              shouldOutput = True

          if shouldOutput:
            outputTuple = self.joinSchema.instantiate(*[joinExpSetup[f] for f in self.joinSchema.fields])
            self.emitOutputTuple(self.joinSchema.pack(outputTuple))

      # Track last output page
      if self.outputPages:
        self.outputPages = [self.outputPages[-1]]

    for leftRelation in self.partitionFiles[0].values():
      self.storage.removeRelation(leftRelation)
    for rightRelation in self.partitionFiles[1].values():
      self.storage.removeRelation(rightRelation)
    self.partitionFiles = {0:{}, 1:{}}

    return self.storage.pages(self.relationId())


  # Hash join helpers
  # TODO assignment
  # Partitions either the left or right side
  def partitionSide(self, left=False):

    if left:
      plan = self.lhsPlan
    else:
      plan = self.rhsPlan

    iterator = iter(plan)
    for (PageId, Page) in iterator:
      for tuple in Page:
        if left:
          tupleSchema = self.lhsSchema
          hashFn = self.lhsHashFn
          partId = 0
        else:
          tupleSchema = self.rhsSchema
          hashFn = self.rhsHashFn
          partId = 1

        joinExpSetup = self.loadSchema(tupleSchema, tuple)
        key = eval(hashFn, globals(), joinExpSetup)
        relationID = "HashJoin" + str(self.id()) + str(left) + str(tuple)

        if not self.storage.hasRelation(relationID):
          self.storage.createRelation(relationID, tupleSchema)
          self.partitionFiles[partId][key] = relationID

        partitionFile = self.storage.fileMgr.relationFile(relationID)[1]
        partitionFile.insertTuple(tuple)



  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      exprs = "(expr='" + str(self.joinExpr) + "')"

    elif self.joinMethod == "indexed":
      exprs =  "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "indexKeySchema=" + self.lhsKeySchema.toString() ]
        ))) + ")"

    elif self.joinMethod == "hash":
      exprs = "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "lhsKeySchema=" + self.lhsKeySchema.toString() ,
            "rhsKeySchema=" + self.rhsKeySchema.toString() ,
            "lhsHashFn='" + self.lhsHashFn + "'" ,
            "rhsHashFn='" + self.rhsHashFn + "'" ]
        ))) + ")"

    return super().explain() + exprs

# Iterates over left and right hand side pages
class PartitionIterator:
  def __init__(self, partFiles, storageEngine):
    self.partFiles = partFiles
    self.storage   = storageEngine
    self.fileIter  = iter(self.partFiles)
    self.nextPair()

  def __iter__(self):
    return self

  def __next__(self):
    if self.fileIter is not None:
      while self.pagePairIter is not None:
        try:
          return next(self.pagePairIter)
        except StopIteration:
          self.nextPair()
    if self.fileIter is None:
      raise StopIteration

  def nextPair(self):
    try:
      (leftRelation, rightRelation) = next(self.fileIter)
      self.leftFile = self.storage.fileMgr.relationFile(leftRelation)[1]
      self.rightFile = self.storage.fileMgr.relationFile(rightRelation)[1]

    except StopIteration:
      self.fileIter     = None
      self.pagePairIter = None

    else:
      self.pagePairIter = itertools.product(self.leftFile.pages(), self.rightFile.pages())
