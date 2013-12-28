#!/usr/bin/python
# vi: ts=4 noexpandtab

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 October 2011
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.


import threading
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
import logging
import Queue

class S3DeleteWorker(threading.Thread):

	def __init__(self, queue, bucket, logger, creds, tname, silent=False):
		self.queue = queue
		self.logger = logger
		self.bucket = bucket
		self.tname = tname
		self.silent = silent
		threading.Thread.__init__(self)

		self.logger.debug("Initializing S3 Connection for %s" % self.tname)

		try:
			self.connection = S3Connection(creds[0], creds[1])
		except Exception, e:
			self.logger.error("Exception during establishment of S3 Connection")
			self.logger.error(e)

		self.logger.debug("S3 Connection established")

	def run(self):
		buck = self.connection.get_bucket( self.bucket )
		self.logger.info("Deleteing files. Silent run: %s" % self.silent)

		while not self.queue.empty():
			name = self.queue.get()

			to_key = buck.get_key( name )
			if to_key is not None and to_key.exists:
				try:
					k = Key(buck)
					k.key = name
					k.delete()
				except Exception, e:
					self.logger.error("FAILED TO DELETE %s" % name)
					self.logger.error(e)

			self.queue.task_done()

			if not self.silent:
				self.logger.debug("D %s from bucket %s" % ( name, self.bucket ))
