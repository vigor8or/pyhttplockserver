#!/usr/bin/env python3

import argparse
import base64
from functools import partial, wraps
from http import HTTPStatus
try:
	# New in version 3.7
	from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
except ImportError:
	from http.server import BaseHTTPRequestHandler, HTTPServer
	import socketserver
	class ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
		daemon_thread = True
import json
import logging
import re
import ssl
import traceback

from urllib.parse import unquote

from lock_manager import LockManager, LockManagerRepeatedAcquire, LockManagerNotFound, LockManagerTimeout

logger = logging.getLogger(__name__)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--interval", default=1)
	parser.add_argument("-p", "--port", default=8000)
	parser.add_argument("-a", "--authentication", required=False, metavar="username:password")
	parser.add_argument("-c", "--certificate", required=False)
	args = parser.parse_args()
	
	loglevels = [logging.INFO, logging.DEBUG]
	verboselevel = loglevels[min(len(loglevels)-1, 0)]
	console_handler = logging.StreamHandler()
	logging.getLogger().addHandler(console_handler)
	logging.getLogger().setLevel(verboselevel)
	
	lockman = LockManager(args.interval)
	lockman_handler = partial(LockHttpHandler, lockman, args.authentication)
	httpd = ThreadingHTTPServer(("", args.port), lockman_handler)
	if args.certificate:
		ssl.wrap_socket(httpd, certfile=args.certificate, server_side=True)
	httpd.serve_forever()

class LockHttpHandler(BaseHTTPRequestHandler):
	DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
	LOCKS_ENDPOINT = r"^/locks"
	HOLDERS_ENDPOINT = r"^/holders"
	LOCK_TYPE_MAP = {"exclusive": LockManager.LOCK_EXCLUSIVE, "shared": LockManager.LOCK_SHARED}
	
	lockmanager = None
	
	def __init__(self, lockmanager, authentication, *args, **kwargs):
		type(self).lockmanager = lockmanager
		self.authentication = authentication
		super(LockHttpHandler, self).__init__(*args, **kwargs)
	
	def _http_wrap(func):
		@wraps(func)
		def wrapper(self, *args, **kwargs):
			try:
				if self.authentication:
					authorization = self.headers.get("Authorization")
					auth_decode = base64.b64decode(authorization[len("Basic "):]).decode("utf-8") if authorization else None
					if not auth_decode:
						self.send_response(HTTPStatus.UNAUTHORIZED.value)
						self.send_header("WWW-Authenticate", 'Basic, charset="UTF-8"')
						self.end_headers()
				
				if not func(self, *args, **kwargs):
					self.send_response(HTTPStatus.NOT_FOUND.value)
					self.end_headers()
			except LockManagerRepeatedAcquire as e:
				self.send_body(HTTPStatus.CONFLICT.value, json.dumps({"message": str(e)}))
			except LockManagerNotFound as e:
				self.send_body(HTTPStatus.NOT_FOUND.value, json.dumps({"message": str(e)}))
			except LockManagerTimeout as e:
				self.send_body(HTTPStatus.REQUEST_TIMEOUT.value, json.dumps({"message": str(e)}))
			except:
				logger.error(traceback.format_exc())
				self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR.value)
				self.end_headers()
		return wrapper

	@_http_wrap
	def do_GET(self):
		locks, holders = type(self).lockmanager.get_state()
		
		lock_fmt = lambda x: {
			"priority": x.priority,
			"client": x.client,
			"lock_type": x.lock_type,
			"request_timestamp": x.request_timestamp.isoformat()
		}
		holder_fmt = lambda x: {
			"lock_type": {v: k for k, v in type(self).LOCK_TYPE_MAP.items()}.get(x.lock_type),
			"client": x.client,
			"acquire_timestamp": x.acquire_timestamp.isoformat()
		}
		
		if re.search(self.LOCKS_ENDPOINT + r"/?$", self.path): # population of all locks
			locks_serialized = {k: [lock_fmt(x) for x in v] for k, v in locks.items()}
			self.send_body(HTTPStatus.OK.value, json.dumps(locks_serialized))
			return True
		m = re.search(self.LOCKS_ENDPOINT + r"/([^/]*)/?$", self.path) # details for single lock
		if m:
			lockname = unquote(m.group(1))
			lock = locks.get(lockname)
			if lock is not None:
				locks_serialized = [lock_fmt(x) for x in lock]
				self.send_body(HTTPStatus.OK.value, json.dumps(locks_serialized))
			else:
				self.send_body(HTTPStatus.NOT_FOUND.value, "no lock of name [{0}] found".format(lockname))
			return True
		if re.search(self.HOLDERS_ENDPOINT + r"/?$", self.path): # population of all actively held locks
			holders_serialized = {k: [holder_fmt(x) for x in v] for k, v in holders.items()}
			
			self.send_body(HTTPStatus.OK.value, json.dumps(holders_serialized))
			return True
		m = re.search(self.HOLDERS_ENDPOINT + r"/([^/]*)/?$", self.path) # details for single actively held lock
		if m:
			lockname = unquote(m.group(1))
			holder = holders.get(lockname)
			if holder is not None:
				holders_serialized = [holder_fmt(x) for x in holder]
				self.send_body(HTTPStatus.OK.value, json.dumps(holders_serialized))
			else:
				self.send_body(HTTPStatus.NOT_FOUND.value, "no lock of name [{0}] found".format(lockname))
			return True
		
		return False
	
	@_http_wrap
	def do_PUT(self):
		content_length = int(self.headers["Content-Length"])
		m = re.search(self.LOCKS_ENDPOINT + r"/([^/]*)/([^/]*)/?$", self.path)
		if m:
			lockname = unquote(m.group(1))
			client = unquote(m.group(2))
			payload = self.rfile.read(content_length)
			jsonload = json.loads(payload)
			priority = jsonload.get("priority")
			timeout = jsonload.get("timeout", 10)
			lock_typename = jsonload.get("type")
			lock_type = type(self).LOCK_TYPE_MAP.get(lock_typename)
			if not lockname or not client or priority is None or not lock_type:
				self.send_body(HTTPStatus.BAD_REQUEST.value)
				return True
			
			try:
				lockrequest = type(self).lockmanager.acquire(lockname, client, priority=priority, timeout=timeout, lock_type=lock_type)
				self.send_body(HTTPStatus.CREATED.value, json.dumps({"message": "lock [{0}] of type [{1}] acquired by client [{2}] with priority [{3}]".format(lockname, lock_typename, client, lockrequest.priority)}))
			except LockManagerRepeatedAcquire:
				# treat locks as re-entrant (idempotent PUT)
				self.send_body(HTTPStatus.OK.value, json.dumps({"message": "NOOP - lock [{0}] has already been acquried by client [{1}]".format(lockname, client)}))
			
			return True
		
		return False
	
	@_http_wrap
	def do_DELETE(self):
		m = re.search(self.LOCKS_ENDPOINT + r"/([^/]*)/([^/]*)/?$", self.path)
		if m:
			lockname = unquote(m.group(1))
			client = unquote(m.group(2))
			if not lockname or not client:
				self.send_body(HTTPStatus.BAD_REQUEST.value)
				return True
			
			type(self).lockmanager.release(lockname, client)
			self.send_body(HTTPStatus.OK.value, json.dumps({"message": "[{0}] released by [{1}]".format(lockname, client)}))
			
			return True
		
		return False

	@_http_wrap
	def do_PATCH(self):
		content_length = int(self.headers["Content-Length"])
		
		m = re.search(self.LOCKS_ENDPOINT + r"/([^/]*)/([^/]*)/?$", self.path)
		if m:
			lockname = unquote(m.group(1))
			client = unquote(m.group(2))
			payload = self.rfile.read(content_length)
			jsonload = json.loads(payload)
			priority = jsonload.get("priority")
			if not lockname or not client or priority is None:
				self.send_body(HTTPStatus.BAD_REQUEST.value)
				return True
			
			old_priority = type(self).lockmanager.modify_priority(lockname, client, priority)
			self.send_body(HTTPStatus.OK.value, json.dumps({"old_priority": old_priority, "message": "[{0}] changed priority on [{1}] from {2} to {3}".format(client, lockname, old_priority, priority)}))
			
			return True
		
		return False
	
	def send_body(self, status, body=None, headers=None):
		if headers is None:
			headers = {"Content-Type": "application/json"}
		
		self.send_response(status)
		for k, v in headers.items():
			self.send_header(k, v)
		self.end_headers()
		if body:
			self.wfile.write(bytes(body, "utf-8"))

if __name__ == "__main__":
	main()
