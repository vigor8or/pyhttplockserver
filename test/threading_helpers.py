from threading import Thread

# modified from https://stackoverflow.com/questions/2829329/catch-a-threads-exception-in-the-caller-thread-in-python
class PropagatingThread(Thread):
	def run(self):
		self.exc = None
		try:
			if hasattr(self, '_Thread__target'):
				# Thread uses name mangling prior to Python 3.
				self.ret = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)
			else:
				self.ret = self._target(*self._args, **self._kwargs)
		except BaseException as e:
			self.exc = e
	
	def join(self, *args, **kwargs):
		super(PropagatingThread, self).join(*args, **kwargs)
		if self.is_alive():
			raise RuntimeError("timed out")
		if self.exc:
			raise self.exc
		return self.ret
