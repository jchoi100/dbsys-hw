import io, math, struct

from collections import OrderedDict
from struct      import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Check initial buffer pool size
  >>> len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  # Default to a 10 MB buffer pool.
  defaultPoolSize = 10 * (1 << 20)

  # Buffer pool constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # pageSize       : the page size to be used with this buffer pool
  # poolSize       : the size of the buffer pool
  def __init__(self, **kwargs):
    self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)
    # Stores the backing binary data
    self.pool         = io.BytesIO(b'\x00' * self.poolSize)

    ####################################################################################
    # DESIGN QUESTION: what other data structures do we need to keep in the buffer pool?

    # Free list keeps track of frameIds in the pool where there are free pages.

    self.freeList = list()

    #Maps a page ID to its corresponding frame ID
    self.frameMap = {}

    #Maps the index of each frameId to an index

    # Put all the empty slots into the free list
    # The frameId * self.pageSize is what starting index it represents
    for x in range(0, self.numPages()):
      self.freeList.append(x)

    self.pageMap = OrderedDict()

  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr

  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize / self.pageSize)

  # TODO assignment ??
  def numFreePages(self):
    return len (self.freeList)

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()


  # Buffer pool operations

  # TODO assignment
  # Josh: Do not change the buffer pool
  def hasPage(self, pageId):
    # Try to find the page in the dictionary
    if pageId in self.pageMap:
      return True

    return False

  # TODO assignment
  # Get page into buffer pool, from the disk ... so this is actually like an insert?

  #Logic according to Josh [OLD]
  #if self.hasPage(id):

  # new logic from Josh
  #def getPage(self, pageId):
  #if pageId in pageMap:
    #return the Page from the pageMap
  #else:
    #frameId = a free frame (from self.freeList. You may need to evict one first)
    #emptyBuffer = the buffer located frame 'frameId' (a memview into self.pool)
    #page = self.fileMgr.readPage(pageId, emptyBuffer) (this returns a Page object)
    #put page in pageMap
    #remove frameId from the self.freeList
    #return page

  def getPage(self, pageId):
    if(self.hasPage(pageId)):
      self.pageMap.move_to_end(pageId)
      return self.pageMap.get(pageId)

    else:
      if len(self.freeList) is 0:
        self.evictPage()

      index = self.freeList.pop() * self.pageSize
      emptyBuffer = self.pool.getbuffer()[index:index + self.pageSize]
      page = self.fileMgr.readPage(pageId, emptyBuffer)

      self.frameMap[pageId] = index
      self.pageMap[pageId] = page
      #self.frameMap.append({pageId: index})
      #self.pageMap.append({pageId : page})
      return page


  # Removes a page from the page map, returning it to the free
  # page list without flushing the page to the disk.
  # TODO assignment
  def discardPage(self, pageId):
    if(self.hasPage(pageId)):
      index = self.frameMap.get(pageId)
      self.freeList.append(math.floor(index / self.pageSize))

      self.frameMap.pop(pageId)
      self.pageMap.pop(pageId)

      #Clear pool at index
      self.pool.getbuffer()[index:index + self.pageSize] = bytes(self.pageSize)

      #index = pageMap.pop(pageId).pageID.fileId.fileIndex

  # TODO assignment
  # Write a page back out to the disk
  def flushPage(self, pageId):
    if(self.hasPage(pageId)):
      self.pageMap.move_to_end(pageId)
      page = self.pageMap.get(pageId)
      self.fileMgr.writePage(page)

    else:
      raise LookupError("Tried to flush a page that does not exist")

  # Evict [just one page] using LRU policy. (Least Recently Used)
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  # TODO assignment
  def evictPage(self):
    #toRemove = next(iter(sorted(self.pageMap)))
    #page index of the least recently used
    (index, page) = self.pageMap.popitem(last=False)
    self.pageMap[index] = page

    if(page.isDirty()):
      self.flushPage(index)

    #self.freeList.append(toRemove)
    self.discardPage(index)



  # Flushes all dirty pages
  # TODO assignment is this the best way to do it?
  def clear(self):
    #it = iter(sorted(pageMap.iteritems()))
    for k,v in self.pageMap.items():
      if v.isDirty():
        self.flushPage(k)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
