import bisect
from collections import namedtuple
from datetime import datetime
import itertools
from threading import Lock
import time

LockRequest = namedtuple("LockRequest", ["priority", "request_timestamp", "lock_type", "client"])
LockHold = namedtuple("LockHold", ["lock_type", "client", "acquire_timestamp"])

class LockManager(object):
	LOCK_EXCLUSIVE = 1
	LOCK_SHARED = 2
	
	def __init__(self, wakeup_interval=1):
		self.locks = dict()
		self.holders = dict()
		self.mutex = Lock()
		self.wakeup_interval = wakeup_interval
	
	def get_state(self):
		with self.mutex:
			return self.locks, self.holders
	
	def client_removed(self, thelist, client):
		counter = itertools.count()
		newlist = [x for x in thelist if x.client != client or (x.client == client and next(counter) != 0)]
		return newlist
	
	def find_client_index(self, locks, client):
		client_index = next((i for (i, tup) in enumerate(locks) if tup.client == client), None)
		return client_index
	
	def acquire(self, name, client, lock_type=LOCK_EXCLUSIVE, priority=2, timeout=1):
		"""
		Attempt to acquire the lock <name> by <client> with a <lock_type> lock
		If a lock is already held by another client, requests are queued by <priority> ascending
		i.e. the lower thee number, the higher the priority
		
		The implementation of exclusive and shared locks can be generalized to assigning numerical vlaues to lock types such that:
			- locks cannot be acquired if the currently held lock is exclusive or have <lock_type> of lower numerical value
			- there is no <lock_type> with lower numerical value than exclusive lock
		
		:raises LockManagerTimeout: if <timeout> is exceeded
		:raises LockManagerRepeatedAcquire: on double-acquire by the same client
		"""
		with self.mutex:
			if next((tup for tup in self.locks.get(name, []) if tup.client == client), None):
				raise LockManagerRepeatedAcquire("acquire request on [{0}] by [{1}] already exists".format(name, client))
			else:
				# add the client to the queue in order of <priority> value
				bisect.insort(self.locks.setdefault(name, []), LockRequest(priority, datetime.now().astimezone(), lock_type, client))
			
		client_index = 0
		start = time.time()
		while True:
			with self.mutex:
				client_index = self.find_client_index(self.locks[name], client)
				if lock_type == self.LOCK_EXCLUSIVE:
					if not self.holders.get(name) and self.locks[name][0].client == client: #if no current holder and the front of the queue is the requesting client
						break
				elif lock_type == self.LOCK_SHARED:
					counter =  itertools.count()
					if not any(x.lock_type < lock_type for x in self.holders.get(name, [])) and not [x for x in self.locks[name] if x.lock_type < lock_type or (x.client == client and next(counter) !=0 )]: # no lower value locks held or preceded by in the request queue
						break
				else:
					raise ValueError("unknown lock type: {0}".format(lock_type))
			time.sleep(self.wakeup_interval)
			if time.time() - start >= timeout:
				# dequeue the client
				with self.mutex:
					requests = self.locks.get(name)
					self.locks[name] = self.client_removed(requests, client)
				raise LockManagerTimeout("{0} request on lock {1} exceeded timeout of {2}s".format(client, name, timeout))
		
		with self.mutex:
			bisect.insort(self.holders.setdefault(name, []), LockHold(lock_type, client, datetime.now().astimezone()))
		
		# return the changed (if any) LockRequest object
		return self.locks[name][client_index]
	
	def release(self, lockname, client):
		"""
		release the lock with <lockname> held by <client>
		an attempt to release a lock not held by a client results in an error
		
		:raises LockManagerNotFound: if <lockname> or <client> does not exist
		"""
		with self.mutex:
			requests = self.locks.get(lockname)
			holding = self.holders.get(lockname)
			if not requests:
				raise LockManagerNotFound("no lock of name [{0}] found".format(lockname))
			if not holding:
				raise LockManagerNotFound("lock [{0}] does not exist or is not being held".format(lockname))
			if requests:
				holding2 = self.client_removed(holding, client)
				if len(holding) != len(holding2):
					self.holders[lockname] = holding2
				else:
					raise LockManagerNotFound("client [{0}] cannot release lock [{1}] as it is not holding it".format(client, lockname))
				
				requests2 = self.client_removed(requests, client)
				if len(requests) != len(requests2):
					self.locks[lockname] = requests2
				else:
					raise LockManagerNotFound("no client [{0}] against lock [{1}] found".format(client, lockname))
			else:
				raise LockManagerNotFound("no lock of name [{0}] found".format(lockname))
	
	def modify_priority(self, name, client, new_priority):
		"""
		Modify a lock's priority to change its placement in the request queue - any ongoing locks are maintained
		
		:raises LockManagerNotFound: if <lockname> or <client> does not exist
		"""
		with self.mutex:
			requests = self.locks.get(name)
			if requests:
				client_index = next((i for (i, tup) in enumerate(requests) if tup.client == client), None)
				if client_index:
					old_request = requests[client_index]
					old_priority = old_request.priority
					new_request = LockRequest(new_priority, datetime.now().astimezone(), old_request.lock_type, old_request.client)
					
					del requests[client_index]
					bisect.insort(self.locks.setdefault(name, []), new_request)
					return old_priority
				else:
					raise LockManagerNotFound("no client of name [{0}] found".format(client))
			else:
				raise LockManagerNotFound("no lock of name [{0}] found".format(name))

class LockManagerNotFound(Exception):
	pass

class LockManagerTimeout(Exception):
	pass

class LockManagerRepeatedAcquire(Exception):
	pass
