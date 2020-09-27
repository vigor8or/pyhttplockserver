#!/usr/bin/env python3

import time
from threading import Event, Lock
import unittest

from lock_manager import LockManager, LockManagerRepeatedAcquire, LockManagerNotFound, LockManagerTimeout
from .threading_helpers import PropagatingThread

class LockManagerTest(unittest.TestCase):
	TESTCASE_TIMEOUT = 10
	
	def setUp(self):
		self.lockm = LockManager()
		self.mutex = Lock()
		self.assertLocks({})
	
	def assertLocks(self, expected):
		self.assertEqual({k: [(x.priority, x.client) for x in v] for k, v in self.getLocks().items()}, expected)
	
	def getLocks(self):
		locks, holders = self.lockm.get_state()
		return locks
	
	def joinThreads(self, funcs):
		threads = [PropagatingThread(target=f) for f in funcs]
		for t in threads:
			t.start()
		for t in threads:
			t.join(self.TESTCASE_TIMEOUT)
		for t in threads:
			if t.is_alive():
				raise RuntimeError("timed out")
	
	def test_timeout(self):
		"""Test lock timeout"""
		def client1():
			self.lockm.acquire("lock1", "client1")
			self.assertLocks({"lock1": [(2, "client1")]})
			
			# client1 still has the lock so expect a timeout
			with self.assertRaises(LockManagerTimeout):
				self.lockm.acquire("lock1", "client2", timeout=2)
			self.assertLocks({"lock1": [(2, "client1")]})
			
			# client1 releases the lock
			self.lockm.release("lock1", "client1")
			self.assertLocks({"lock1": []})
		
		self.joinThreads([client1])
	
	def test_acquire_and_release(self):
		"""Test simple acquire and release sequence with 2 clients
		Expected sequence of events:
		1. client1 acquires lock
		2. client2 acquire request (waits)
		3. client1 releases lock
		4. client2 acquires lock
		"""
		event1 = Event()
		event2 = Event()
		def client1():
			self.lockm.acquire("lock1", "client1")
			self.assertLocks({"lock1": [(2, "client1")]})
			event1.set()
			
			self.lockm.release("lock1", "client1")
			
			event2.wait()
			self.assertLocks({"lock1": [(2, "client2")]})
		
		def client2():
			event1.wait()
			self.lockm.acquire("lock1", "client2")
			
			event2.set()
			self.assertLocks({"lock1": [(2, "client2")]})
		
		self.joinThreads([client1, client2])
	
	def test_priority(self):
		"""Test priority lock behaviours
		1) a higher priority (lower number) lock acquires before lower priority locks
		2) a higher priority lock request still waits on an already acuqired lower priority lock (until release)
		Expected sequence of events:
		1. client1 acquires lock
		2. client2 acquire request (waits)
		3. client3 acquire request (waits)
		4. client1 releases lock (client3 cannot acquire until after this - property 2)
		5. client3 acquires lock (ahead of client2 - property 1)
		6. client3 releases lock
		7. client2 releases lock
		8. client2 releases lock
		"""
		event1 = Event()
		def client1():
			self.lockm.acquire("lock1", "client1", priority=1, timeout=10)
			self.assertLocks({"lock1": [(1, "client1")]})
			event1.set()
			
			# wait for client2 and client3 acquire request
			while len(self.getLocks().get("lock1")) <= 2:
				time.sleep(1)
			self.assertLocks({"lock1": [(0, "client3"), (1, "client1"), (2, "client2")]})
			
			self.lockm.release("lock1", "client1")
		
		def client2():
			event1.wait()
			self.lockm.acquire("lock1", "client2", priority=2, timeout=10)
			self.assertLocks({"lock1": [(2, "client2")]})
			
			self.lockm.release("lock1", "client2")
			self.assertLocks({"lock1": []})
		
		def client3():
			event1.wait()
			# waitfor client2 acquire request
			while len(self.getLocks().get("lock1")) <= 1:
				time.sleep(1)
			self.lockm.acquire("lock1", "client3", priority=0, timeout=10)
			self.assertLocks({"lock1": [(0, "client3"), (2, "client2")]})
			
			self.lockm.release("lock1", "client3")
		
		self.joinThreads([client1, client2, client3])
	
	def test_priority_change(self):
		"""Test priority lock with intermediate priority change
		Expected sequence of events:
		1. client1 acquires lock
		2. client2 and client3 acquire request with client2 priority above client3
		3. increase priority client3 above client2
		4. client1 releases lock
		5. client3 acquires lock
		6. client3 releases lock
		7. client2 acquires lock
		8. client2 releases lock
		"""
		event1 = Event()
		event2 = Event()
		def client1():
			self.lockm.acquire("lock1", "client1", priority=1, timeout=10)
			self.assertLocks({"lock1": [(1, "client1")]})
			event1.set()
			
			event2.wait()
			self.lockm.release("lock1", "client1")
		
		def client2():
			event1.wait()
			self.lockm.acquire("lock1", "client2", priority=2, timeout=10)
			self.assertLocks({"lock1": [(2, "client2")]})
			
			self.lockm.release("lock1", "client2")
			self.assertLocks({"lock1": []})
		
		def client3_1():
			event1.wait()
			self.lockm.acquire("lock1", "client3", priority=3, timeout=10)
			self.assertLocks({"lock1": [(0, "client3"), (2, "client2")]})
			
			self.lockm.release("lock1", "client3")
		
		def client3_2():
			event1.wait()
			# wait for client1, client2 and client3 acquire request
			while len(self.getLocks().get("lock1")) <= 1:
				time.sleep(1)
			self.assertLocks({"lock1": [(1, "client1"), (2, "client2"), (3, "client3")]})
			
			self.lockm.modify_priority("lock1", "client3", new_priority=0)
			self.assertLocks({"lock1": [(0, "client3"), (1, "client1"), (2, "client2")]})
			event2.set()
		
		self.joinThreads([client1, client2, client3_1, client3_2])
	
	def test_double_acquire(self):
		"""Test double acquire behaviour"""
		def client1():
			self.lockm.acquire("lock1", "client1")
			self.assertLocks({"lock1": [(2, "client1")]})
			
			# client1 still has the lock so expect a timeout
			with self.assertRaises(LockManagerRepeatedAcquire):
				self.lockm.acquire("lock1", "client1")
			
		self.joinThreads([client1])
	
	def test_shared(self):
		"""Test exclusive lock and shared lock interaction"""
		def client1():
			# shared lock cannot be acquired when exclusive lock present
			self.lockm.acquire("lock1", "exclusive1", lock_type=LockManager.LOCK_EXCLUSIVE)
			with self.assertRaises(LockManagerTimeout):
				self.lockm.acquire("lock1", "shared1", lock_type=LockManager.LOCK_SHARED)
			self.assertLocks({"lock1": [(2, "exclusive1")]})
			
			# multiple shared locks can be acquired
			self.lockm.release("lock1", "exclusive1")
			self.lockm.acquire("lock1", "shared1", lock_type=LockManager.LOCK_SHARED)
			self.assertLocks({"lock1": [(2, "shared1")]})
			self.lockm.acquire("lock1", "shared2", lock_type=LockManager.LOCK_SHARED)
			self.assertLocks({"lock1": [(2, "shared1"), (2, "shared2")]})
			
			# exclusive lock cannot be acquired when shared locks present
			with self.assertRaises(LockManagerTimeout):
				self.lockm.acquire("lock1", "exclusive1", lock_type=LockManager.LOCK_EXCLUSIVE)
			
			self.lockm.release("lock1", "shared1")
			self.lockm.release("lock1", "shared2")
			self.lockm.acquire("lock1", "exclusive1", lock_type=LockManager.LOCK_EXCLUSIVE)
			self.lockm.release("lock1", "exclusive1")
			self.assertLocks({"lock1": []})
		
		self.joinThreads([client1])
	
	def test_keep_single(self):
		"""Test client1 retains lock independent of client2 and client3 actions
		Expected sequence of events:
		1. cleint1 acquires lock
		2. client2 acquire request (waits as client1 acquired)
		3. client2 release requests (should not cause the release of client1)
		4. client3 acquire request (should timeout because client1 still holds the lock)
		5. client1 release lock
		"""
		event1 = Event()
		event2 = Event()
		def client1():
			self.lockm.acquire("lock1", "client1")
			self.assertLocks({"lock1": [(2, "client1")]})
			event1.set()
			
			while len(self.getLocks().get("lock1")) <= 1:
				time.sleep(1)
			
			with self.assertRaises(LockManagerNotFound):
				self.lockm.release("lock1", "client2")
			
			with self.assertRaises(LockManagerTimeout):
				self.lockm.acquire("lock1", "client3")
			
			event2.wait()
			self.assertLocks({"lock1": [(2, "client1")]})
			self.lockm.release("lock1", "client1")
			self.assertLocks({"lock1": []})
		
		def client2():
			event1.wait()
			with self.assertRaises(LockManagerTimeout):
				self.lockm.acquire("lock1", "client2")
			event2.set()
		
		self.joinThreads([client1, client2])
