#!/usr/bin/env python3

from datetime import datetime
from functools import partial
from http import HTTPStatus
import http.server
import json
import os
import os.path
from threading import Event
from urllib.request import HTTPError, Request, urlopen
import unittest
from unittest.mock import patch

from http_lock_server import LockHttpHandler
from lock_manager import LockManager, LockManagerRepeatedAcquire, LockRequest, LockHold
from .threading_helpers import PropagatingThread

class HTTPLockServerTest(unittest.TestCase):
	
	DEFAULT_TIMESTAMP = datetime.now().astimezone()
	
	def setUp(self):
		server_ready = Event()
		self.stop_server = Event()
		
		self.lockm = LockManager()
		lock_handler = partial(LockHttpHandler, self.lockm, None)
		test_http_server = partial(DisposableThreadingHttpServer, server_ready, self.stop_server)
		self.port = os.environ.get("TEST_PORT", 8000)
		self.ROOT_URL = "http://localhost:" + str(self.port)
		self.server_thread = PropagatingThread(target=http.server.test, daemon=True, kwargs={"HandlerClass": lock_handler, "ServerClass": test_http_server, "port": self.port})
		self.server_thread.start()
		
		# wait until the server_thread has finished starting
		server_ready.wait()
	
	def tearDown(self):
		self.stop_server.set()
		# wait for the server_thread to shutdown (ignore errors)
		try:
			self.server_thread.join()
		except:
			pass
	
	def test_GET(self):
		self.lockm.locks = {
			"lock1": [LockRequest(1, self.DEFAULT_TIMESTAMP, LockManager.LOCK_EXCLUSIVE, "client1")]
		}
		with urlopen(self.ROOT_URL + "/locks") as f:
			response = f.read().decode("utf-8")
			self.assertEqual(json.loads(response), {"lock1": [{"priority": 1, "request_timestamp": self.DEFAULT_TIMESTAMP.isoformat(), "lock_type": LockManager.LOCK_EXCLUSIVE, "client": "client1"}]})
	
	def test_GET_single(self):
		self.lockm.locks = {
			"lock1": [LockRequest(1, self.DEFAULT_TIMESTAMP, LockManager.LOCK_EXCLUSIVE, "client1")]
		}
		with urlopen(self.ROOT_URL + "/locks/lock1") as f:
			response = f.read().decode("utf-8")
			self.assertEqual(json.loads(response), [{"priority": 1, "request_timestamp": self.DEFAULT_TIMESTAMP.isoformat(), "lock_type": LockManager.LOCK_EXCLUSIVE, "client": "client1"}])
	
	def test_GET_holders(self):
		self.lockm.holders = {
			"lock1": [
				LockHold(LockManager.LOCK_SHARED, "client1", self.DEFAULT_TIMESTAMP),
				LockHold(LockManager.LOCK_SHARED, "client2", self.DEFAULT_TIMESTAMP)
			]
		}
		
		with urlopen(self.ROOT_URL + "/holders") as f:
			response = f.read().decode("utf-8")
			self.assertEqual(
				json.loads(response),
				{
					"lock1": [
						{"lock_type": "shared", "client": "client1", "acquire_timestamp": self.DEFAULT_TIMESTAMP.isoformat()},
						{"lock_type": "shared", "client": "client2", "acquire_timestamp": self.DEFAULT_TIMESTAMP.isoformat()}
					]
				}
			)
	
	def test_GET_holders_single(self):
		self.lockm.holders = {
			"lock1": [
				LockHold(LockManager.LOCK_EXCLUSIVE, "client1", self.DEFAULT_TIMESTAMP),
			]
		}
		
		with urlopen(self.ROOT_URL + "/holders/lock1") as f:
			response = f.read().decode("utf-8")
			self.assertEqual(
				json.loads(response),
				[
					{"lock_type": "exclusive", "client": "client1", "acquire_timestamp": self.DEFAULT_TIMESTAMP.isoformat()},
				]
			)
	
	def test_GET_locks_404(self):
		self.lockm.locks = {}
		with self.assertRaises(HTTPError) as raised:
			urlopen(self.ROOT_URL + "/locks/notfound")
		self.assertEqual(raised.exception.getcode(), HTTPStatus.NOT_FOUND.value)
	
	@patch.object(LockManager, "get_state")
	def test_GET_500(self, mock):
		mock.side_effect = Exception()
		with self.assertRaises(HTTPError) as raised:
			urlopen(self.ROOT_URL + "/locks")
		self.assertEqual(raised.exception.getcode(), HTTPStatus.INTERNAL_SERVER_ERROR.value)
	
	@patch.object(LockManager, "acquire")
	def test_PUT(self, _):
		with urlopen(Request(self.ROOT_URL + "/locks/lock1/client1", data='{"priority":1, "type":"exclusive"}'.encode("utf-8"), method="PUT")) as f:
			self.assertEqual(f.getcode(), HTTPStatus.CREATED.value)
	
	@patch.object(LockManager, "acquire")
	def test_PUT_repeated(self, mock):
		mock.side_effect = LockManagerRepeatedAcquire()
		with urlopen(Request(self.ROOT_URL + "/locks/lock1/client1", data='{"priority":1, "type":"exclusive"}'.encode("utf-8"), method="PUT")) as f:
			self.assertEqual(f.getcode(), HTTPStatus.OK.value)
	
	def test_PUT_401(self):
		with self.assertRaises(HTTPError) as raised:
			urlopen(Request(self.ROOT_URL + "/locks/lock1/client1", data='{}'.encode("utf-8"), method="PUT"))
		self.assertEqual(raised.exception.getcode(), HTTPStatus.BAD_REQUEST.value)
	
	@patch.object(LockManager, "acquire")
	def test_PUT_500(self, mock):
		mock.side_effect = Exception()
		with self.assertRaises(HTTPError) as raised:
			urlopen(Request(self.ROOT_URL + "/locks/lock1/client1", method="PUT"))
		self.assertEqual(raised.exception.getcode(), HTTPStatus.INTERNAL_SERVER_ERROR.value)
	
	@patch.object(LockManager, "release")
	def test_DELETE(self, _):
		with urlopen(Request(self.ROOT_URL + "/locks/lock1/client1", data='{"priority":1, "type":"exclusive"}'.encode("utf-8"), method="DELETE")) as f:
			self.assertEqual(f.getcode(), HTTPStatus.OK.value)
	
	@patch.object(LockManager, "modify_priority", return_value=1)
	def test_PATCH(self, _):
		with urlopen(Request(self.ROOT_URL + "/locks/lock1/client1", data='{"priority":0}'.encode("utf-8"), method="PATCH")) as f:
			self.assertEqual(f.getcode(), HTTPStatus.OK.value)
	
	def test_404_generic(self):
		with self.assertRaises(HTTPError) as raised:
			urlopen(self.ROOT_URL + "/notfound")
		self.assertEqual(raised.exception.getcode(), HTTPStatus.NOT_FOUND.value)

class DisposableThreadingHttpServer(http.server.ThreadingHTTPServer):
	def __init__(self, server_ready, stop_server, *args, **kwargs):
		super(DisposableThreadingHttpServer, self).__init__(*args, **kwargs)
		self.server_ready = server_ready
		self.stop_server = stop_server
	
	def service_actions(self):
		if not self.server_ready.is_set():
			self.server_ready.set()
		if self.stop_server.is_set():
			raise KeyboardInterrupt()
