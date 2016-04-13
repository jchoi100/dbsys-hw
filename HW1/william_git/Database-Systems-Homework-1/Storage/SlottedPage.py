import functools, math, struct
from struct import Struct
from io     import BytesIO

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page

####
# James Choi
####

###########################################################
# DESIGN QUESTION 1: should this inherit from PageHeader?
# If so, what methods can we reuse from the parent?
#
class SlottedPageHeader(PageHeader):
  """
  A slotted page header implementation. This should store a slot bitmap
  implemented as a memoryview on the byte buffer backing the page
  associated with this header. Additionally this header object stores
  the number of slots in the array, as well as the index of the next
  available slot.

  The binary representation of this header object is: (numSlots, nextSlot, slotBuffer)

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = SlottedPageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = SlottedPageHeader.unpack(buffer.getbuffer())

  ## Dirty bit tests
  >>> ph.isDirty()
  False
  >>> ph.setDirty(True)
  >>> ph.isDirty()
  True
  >>> ph.setDirty(False)
  >>> ph.isDirty()
  False

  ## Tuple count tests
  >>> ph.hasFreeTuple()
  True

  # First tuple allocated should be at the first slot.
  # Notice this is a slot index, not an offset as with contiguous pages.
  >>> ph.nextFreeTuple() == 0
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
  
  >>> ph.numTuples() == tuplesToTest+1
  True

  >>> ph.hasFreeTuple()
  True

  # Check space utilization
  >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  True

  >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
  True

  >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  [11, 12, ...]

  >>> ph.hasFreeTuple()
  False

  # No value is returned when trying to exceed the page capacity.
  >>> ph.nextFreeTuple() == None
  True
  
  >>> ph.freeSpace() < ph.tupleSize
  True
  """

  slotContentsFormat = "H"
  slotContents = struct.Struct(slotContentsFormat)
  numSlots = None
  slots = None

  def __init__(self, **kwargs):
    buffer     = kwargs.get("buffer", None)
    self.flags = kwargs.get("flags", b'\x00')
    if buffer:
      super().__init__(**kwargs)
      self.tupleSize = kwargs.get("tupleSize", 0)
      self.numSlots = self.numMaxTuples()
      self.slots = self.makeSlots(buffer)
      self.binrepr = struct.Struct(self.slotContentsFormat + str(self.slotBitvectorLen()) + "s")
      startOffset = PageHeader.size
      endOffset = startOffset + self.slotContents.size
      buffer[startOffset:endOffset] = self.slotContents.pack(self.numSlots)
      #self.slots = kwargs.get("slots", b'\x00' * self.slotBitvectorLen())
      #self.slots[:] = b'\x00' * self.slotBitvectorLen()
      self.reprSize = PageHeader.size + self.binrepr.size
    else:
      raise ValueError("No backing buffer supplied for SlottedPageHeader")

  def makeSlots(self, buffer):
    if self.numSlots:
      start = PageHeader.size + self.slotContents.size
      end = start + self.slotBitvectorLen()
      return memoryview(buffer[start:end])

  def slotBitvectorLen(self):
    if self.numSlots:
      return math.ceil(self.numSlots / 8)

  def numMaxTuples(self):
    headerSize = PageHeader.size + self.slotContents.size
    return math.floor((self.pageCapacity - headerSize) / (self.tupleSize + (1/8)))

  def __eq__(self, other):
    return(PageHeader.__eq__(self, other) and self.numSlots == other.numSlots and self.slots == other.slots)

  def __hash__(self):
    return hash((self.flags, self.tupleSize, self.pageCapacity, self.freeSpaceOffset, self.numSlots, self.nextSlot, self.slotBuffer))

  def headerSize(self):
    return self.reprSize

  # Flag operations.
  def flag(self, mask):
    return (ord(self.flags) & mask) > 0

  def setFlag(self, mask, set):
    if set:
      self.flags = bytes([ord(self.flags) | mask])
    else:
      self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
  def isDirty(self):
    return self.flag(PageHeader.dirtyMask)

  def setDirty(self, dirty):
    self.setFlag(PageHeader.dirtyMask, dirty)

  def numTuples(self):
    return len(list(self.usedSlots()))

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    return self.pageCapacity - self.headerSize() - self.usedSpace()

  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    numSlotsUsed = 0
    for i in self.usedSlots():
      numSlotsUsed += 1
    return numSlotsUsed * self.tupleSize


  # Slot operations.
  def offsetOfSlot(self, slot):
    if len(slot) < 4:
      bffer = BytesIO(bytes(len(slot)))
      tempSlot = bffer.getbuffer
      tempSlot[:len(slot)] = slot[:len(slot)]
      slot = BytesIO(bytes(4)).getbuffer()
      slot[:len(tempSlot)] = temp[:len(temp)]
    num = Struct("I").unpack(slot)[0]
    for i in range(32):
      if (num & pow(2,i)) == 0:
        return i
    return None

  def slotByteOffset(self, slotIndex):
    return math.floor(slotIndex / 8)

  def slotBitOffset(self, slotIndex):
    return slotIndex % 8

  def hasSlot(self, slotIndex):
    slotOffset = self.slotByteOffset(slotIndex)
    return (slotOffset >= 0) and (slotOffset < self.slots.nbytes)

  def getSlot(self, slotIndex):
    if self.hasSlot(slotIndex):
      bytewiseIndex = self.slotByteOffset(slotIndex)
      bitwiseIndex = self.slotBitOffset(slotIndex)
      return bool(self.slots[bytewiseIndex] & (0B1 << bitwiseIndex))

  def setSlot(self, slotIndex, slot):
    if self.hasSlot(slotIndex):
      bytewiseIndex = self.slotByteOffset(slotIndex)
      bitwiseIndex = self.slotBitOffset(slotIndex)
      if slot:
        self.slots[bytewiseIndex] = self.slots[bytewiseIndex] | (0B1 << bitwiseIndex)
      else:
        self.slots[bytewiseIndex] = self.slots[bytewiseIndex] & ~(0B1 << bitwiseIndex)

  def resetSlot(self, slotIndex):
    self.setSlot(slotIndex, False)

  def freeSlots(self):
    freeSlotList = []
    for i in range(self.numSlots):
      if not self.getSlot(i):
        freeSlotList.append(i)
    return freeSlotList

  def usedSlots(self):
    usedSlotList = []
    for i in range(self.numSlots):
      if self.getSlot(i):
        usedSlotList.append(i)
    return usedSlotList

  # Tuple allocation operations.

  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    freeSlotList = self.freeSlots()
    if len(freeSlotList) != 0:
      return True
    else:
      return False

  # Returns the tupleIndex of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    nextFreeIndex = None
    for i in self.freeSlots():
      nextFreeIndex = i
      self.setSlot(nextFreeIndex, True)
      break
    return nextFreeIndex

  def nextTupleRange(self):
    nextFreeIndex = self.nextFreeTuple()
    start = self.headerSize() + self.tupleSize * nextFreeIndex
    end = start + self.tupleSize
    return (nextFreeIndex, start, end)

  # Create a binary representation of a slotted page header.
  # The binary representation should include the slot contents.
  def pack(self):
    if self.numSlots and self.slots:
      return super().pack() + self.binrepr.pack(self.numSlots, self.slots.tobytes())
    else:
      return super().pack()

  # Create a slotted page header instance from a binary representation held in the given buffer.
  @classmethod
  def unpack(cls, buffer):
    inheritedContents = PageHeader.unpack(buffer)
    return cls(parent=inheritedContents, buffer=buffer)



######################################################
# DESIGN QUESTION 2: should this inherit from Page?
# If so, what methods can we reuse from the parent?
#
class SlottedPage(Page):
  """
  A slotted page implementation.

  Slotted pages use the SlottedPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  A slotted page interprets the tupleIndex field in a TupleId object as
  a slot index.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  >>> p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  >>> [schema.unpack(tup).age for tup in p]
  []

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  >>> tId.tupleIndex
  0

  >>> p.header.numTuples()
  1

  # Retrieve the previous tuple
  >>> e2 = schema.unpack(p.getTuple(tId))
  >>> e2
  employee(id=1, age=25)

  # Update the tuple.
  >>> e1 = schema.instantiate(1,28)
  >>> p.putTuple(tId, schema.pack(e1))

  >>> p.header.numTuples()
  1

  # Retrieve the update
  >>> e3 = schema.unpack(p.getTuple(tId))
  >>> e3
  employee(id=1, age=28)

  # Compare tuples
  >>> e1 == e3
  True

  >>> e2 == e3
  False

  # Check number of tuples in page
  >>> p.header.numTuples() == 1
  True

  >>> p.header.numTuples()
  1

  # Add some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples()
  11

  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0)
  >>> sizeBeforeClear = p.header.usedSpace()  
  >>> p.clearTuple(tId)
  
  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  >>> [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  >>> sizeBeforeRemove = p.header.usedSpace()
  >>> p.deleteTuple(tId)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]
  
  # Check that the page's slots have tracked the deletion.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  """

  headerClass = SlottedPageHeader

  # Slotted page constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # buffer       : a byte string of initial page contents.
  # pageId       : a PageId instance identifying this page.
  # header       : a SlottedPageHeader instance.
  # schema       : the schema for tuples to be stored in the page.
  # Also, any keyword arguments needed to construct a SlottedPageHeader.

  def __init__(self, **kwargs):
    buffer = kwargs.get("buffer", None)
    if buffer:
      BytesIO.__init__(self, buffer)
      self.pageId = kwargs.get("pageId", None)
      header      = kwargs.get("header", None)
      schema      = kwargs.get("schema", None)

      if self.pageId and header:
        self.header = header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")

      self.schema = schema

    else:
      raise ValueError("No backing buffer provided to page constructor.")

  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return SlottedPageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a slotted page.")

  # Tuple iterator.
  def __iter__(self):
    if self.header.numTuples() == 0:
      self.iterTupleIndex = 0
      self.numTuplesFound = 0
    else:
      for i in range(self.header.numSlots):
        if self.header.getSlot(i):
          self.iterTupleIndex = i
          self.numTuplesFound = 0
          break
    return self

  def __next__(self):
    t = self.getTuple(TupleId(self.pageId, self.iterTupleIndex))
    if t:
      self.numTuplesFound += 1
      if self.numTuplesFound == self.header.numTuples() + 1:
        raise StopIteration
      else:
        for i in range(self.iterTupleIndex + 1, self.header.numSlots):
          if self.header.hasSlot(i):
            self.iterTupleIndex = i
            break
      return t
    else:
      raise StopIteration

  # Tuple accessor methods

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):
    tupleIdx = tupleId.tupleIndex
    tupleSz = self.header.tupleSize
    if self.header.hasSlot(tupleIdx) and tupleIdx < self.header.numSlots:
      start = self.header.headerSize() + tupleIdx * tupleSz
      return self.getbuffer()[start:start+tupleSz]
    else:
      return None

  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
    return super().putTuple(tupleId, tupleData)

  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
    return super().insertTuple(tupleData)

  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
    return super().clearTuple(tupleId)

  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    if self.header and tupleId:
      self.clearTuple(tupleId)
      self.header.resetSlot(tupleId.tupleIndex)
      #start = self.header.headerSize() + tupleId.tupleIndex * self.header.tupleSize
      #self.getbuffer()[start:start+self.header.tupleSize] = bytes(self.header.tupleSize)

  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):
    buffer = self.getbuffer()
    if self.isDirty():
      self.setDirty(False)
      packedHeader = self.header.pack()
      buffer[0:len(packedHeader)] = packedHeader
    tupleSz = self.header.tupleSize
    for t in self:
      if self.header.numTuples() == 0:
        tupleIdx = self.header.headerSize() + self.iterTupleIndex * tupleSz
        buffer[tupleIdx:tupleIdx + tupleSz] = t
      else:
        tupleIdx = self.header.headerSize() + (self.iterTupleIndex - 1) * tupleSz
        buffer[tupleIdx:tupleIdx + tupleSz] = t
    return buffer

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    header = SlottedPageHeader.unpack(buffer)
    return cls(pageId=pageId, buffer=buffer, header=header)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
