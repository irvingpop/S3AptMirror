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
import boto.exception
import logging
import Queue
import time
import sys
import re

class S3SyncWorker(threading.Thread):

	def __init__(self, queue, done_signal, dest_bucket, logger, creds, pre_checked=False):
		threading.Thread.__init__(self)
		self.setDaemon(True)
		self.pre_checked = pre_checked
		self.queue = queue
		self.logger = logger
		self.done_signal = done_signal
		self.dest_bucket = dest_bucket

		self.logger.debug("Initializing S3 Connection for %s" % self.name)

		try:
			self.connection = S3Connection(creds[0], creds[1])
		except Exception, e:
			self.logger.error("Exception during esatblishment of S3 Connection")
			self.logger.error(e)

		self.logger.debug("%s now has a S3 Connection" % self.name)

	def run(self):
		to_buck = self.connection.get_bucket( self.dest_bucket )
		self.logger.debug("%s found %s items in initial queue" % ( self.name, self.queue.qsize() ))
		okay_to_die = False

		while not okay_to_die:

			'''check if work done has been signaled'''
			while self.queue.empty():

				if self.done_signal.isSet() and self.queue.empty():
					self.logger.debug("%s has no work to do and signal to die received" % self.name )
					okay_to_die = True
					break
				elif self.done_signal.isSet():
					self.logger.deubg("%s received signal, but work remains" % self.name )
					break

			while not self.queue.empty():
				'''Do the work now'''

				item = None
				try:
					item = self.queue.get_nowait()
					self.queue.task_done()
				except Queue.Empty, e:
					self.logger.debug("%s another thread stole my item, continuing" % self.name )
					continue

				local = item[0]
				name = item[1]
				etag = item[2]
				etag_encoded = item[3]
				md5_meta = etag, etag_encoded
				key_names = []
				key_names.append( name )

				if name.find("+") > 0:
					key_names.append( name.repalce("+", ' ') )
					self.logger.info("%s - S3 Hack enabled for %s" % ( self.name, name ))

				if not self.pre_checked:
					to_key = to_buck.get_key( name )
					if to_key is not None and to_key.exists:
						remote_etag = to_key.etag.replace('"','')
						if remote_etag == etag:
							self.logger.info("%s - skipping %s as current" % ( self.name, name ))
							self.queue.task_done()
							continue
						else:
							self.logger.warn("%s - %s for bucket %s has mismatched MD5 \n    REMOTE %s | LOCAL %s" %
								( self.name, name, self.dest_bucket, remote_etag, etag ))
				try:
					k = Key(to_buck)
					k.key = name
					k.set_contents_from_filename( local, md5=md5_meta)
					self.logger.info("%s - UPLOADED  %s to bucket %s" % ( self.name, name, self.dest_bucket ))

					'''Any key with a "+", s3 treats like its a space, cheap hack'''
					if name.find("+") > 0:
						k1 = Key(to_buck)
						k1.key = name

						# TODO (irving): Figure out if/why this is still needed.  Don't need X3 storage usage if we can help it
						# '''fix for wget/curl type clients'''
						# k1.copy( self.dest_bucket, name.replace("+", " ") )
						# self.logger.info("%s - EXTRA copy as %s" % ( self.name, name.replace("+"," ") ))

						# '''fix for apt/iso-8859-1'''
						# k1.copy( self.dest_bucket, name.replace("+", "%2B") )
						# self.logger.info("%s - EXTRA copy as %s" % ( self.name, name.replace("+","%2B") ))

				except boto.exception.AWSConnectionError, e:
					self.logger.error("FAILED connection %s" % self.name )

				except boto.exception.S3CreateError, e:
					self.logger.error("Failed to create key %s" % name )
					self.logger.error(e)
					self.queue.put( item )

				except boto.exception.S3ResponseError, e:
					self.logger.error("Got a bad response from S3 for  %s" % name )
					self.logger.error(e)
					self.queue.put( item )

				except Exception, e:
					self.logger.error("FAILED for some unknown reason %s" % name )
					self.logger.error(e)
					self.queue.put( item )

		self.logger.debug("%s finished work." % self.name )
