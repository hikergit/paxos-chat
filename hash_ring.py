import os
import hashlib


class hash_ring(object):

  def __init__(self, numShards=1):
    self.num_shards = 1
    self.shards = [{'start':0,'end':1,'id':0}]

    for shard in range(numShards-1):
      self.add_shard()

  def add_shard(self):
    #Walk through shards. Find shard with longest domain
    #Split that in half. Update list. Return previous shard number
    longest = -1
    s = (-1,-1,-1)
    pos = -1

    for idx,shard in enumerate(self.shards):
      if shard['end']-shard['start'] > longest:
        longest = shard['end']-shard['start']
        s = shard
        pos = idx

    self.num_shards += 1
    mid = (s['end']-s['start'])/2.0
    mid = s['start'] + mid

    #Create new shard. Starts at mid point
    new_shard = {'start':mid, 'end':s['end'], 'id':self.num_shards-1}

    #Update old shard's end to be at mid
    old_shard = self.shards[pos]
    old_shard['end'] = mid

    self.shards.append(new_shard)
    return old_shard['id']

  def getShard(self, key):
    val = self._hash(str(key))
    for shard in self.shards:
      if val >= shard['start'] and val <= shard['end']:
        return shard['id']

  def _hash(self, key):
    #Returns a hash in the range [0, 1)
    return (int(hashlib.md5(key).hexdigest(),16) % 1000000) / 1000000.0



#Example of using CH
'''
ch = hash_ring(2)

print ch.num_shards

print ch.getShard(10)
print ch.getShard('hello world again')
print ch.getShard(2)
print ch.getShard(10)
'''
