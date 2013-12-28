#!/usr/bin/python
# vi: ts=4 noexpandtab

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 03 October 2011
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.
import sys

import threading
import tempfile
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from boto.exception import S3CreateError, S3ResponseError, S3PermissionsError, S3DataError, BotoClientError, BotoServerError, StorageResponseError
from s3uploadobj import S3UploadObject
import gc
import logging
import os
import Queue
import time
import re
import urllib3
import time

class HTTP2S3Worker(threading.Thread):

	def __init__(self, queue, dest_bucket, logger, creds, error, populated, max_retry=5, pre_checked=False):
		threading.Thread.__init__(self)
		self.setDaemon(True)
		self.queue = queue
		self.dest_bucket = dest_bucket
		self.logger = logger
		self.creds = creds
		self.error = error
		self.populated = populated
		self.max_retry = max_retry		# How many times to try and fetch the file
		self.pre_checked = pre_checked	# Check the destination file for MD5 sum or not

		# Hopefully we re-use the same logger here
		self.logger = logging.getLogger(logger)
		self.logger.info("Worker thread created")

	def reset_s3_connection(self):
		self.logger.info("(re)Establishing S3 connection")
		conn = S3Connection(self.creds[0], self.creds[1])
		to_buck = conn.get_bucket(self.dest_bucket)
		return conn, to_buck, 0

	def run(self):
		"""
		This streams items over HTTP directly to S3 from a Queue comprising tuples of path and MD5 sum
		"""
		last_key = None
		key_count = 0
		to_buck = None
		conn = None

		while not self.error.is_set():

			'''Do the work now'''
			tries = 0
			item = None
			success = False

			if self.queue.empty() and \
				self.populated.is_set():
				self.logger.info("Nothing more to do than die.")
				break

			if self.queue.empty():
				time.sleep(2)
				continue


			'''defend against queue race conditions'''
			try:
				item = self.queue.get()
				name = item.get_value('key_name')				# the item name
				last_key = name									# the last item worked on

				md5 = item.get_value('remote_md5')				# md5 to compare with after writing
				rname = item.get_value('key_name')				# s3 destination key name
				remote_url = item.get_value('remote_url')	 	# remote URL of object
				cache_file = item.get_value('cache_file')		# locale cache
				content_type = item.get_value('content_type')	# Content type of local cache file
				key_count += 1

			except Queue.Empty, e:
				if not self.populated.is_set():
					self.logger.warn("Queue is empty and no more to upload")
					break

				else:
					continue
					pass

			except Exception, e:
				self.logger.warn("Problem fetching from queue: %s" % e)
				continue

			finally:
				pass

			'''reset the key counter'''
			if key_count > 90 or \
				not conn or \
				not to_buck:
				conn, to_buck, key_count = self.reset_s3_connection()

			'''retry logic, yeah!'''
			while tries <= self.max_retry and not success and name:
				try:

					'''check if key exists first'''
					tries += 1
					upload_anyway = True
					upload_stack = []
					valid_keys = [ rname ]
					resp = None

					if rname.find('+') > 0:
						valid_keys.append(rname.replace('+',' '))
						valid_keys.append(rname.replace('+','%2B'))

					for r in valid_keys:
						to_key = to_buck.get_key( r )
						if to_key is not None and to_key.exists():
							remote_etag = to_key.etag.replace('"','')
							if remote_etag == md5:
								if to_key.get_metadata("delete_ordinal"):
									meta={}
									meta['original_delete_ordinal'] = to_key.get_metadata("delete_ordinal")
									to_key.copy( self.dest_bucket, r, metadata=meta, preserve_acl=True)
									self.logger.info("Prevented %s from being deleted" % r)
									upload_stack.append(False)

								else:
									upload_stack.append(False)

							else:
								self.logger.info("Re-uploading %s due to failed MD5" % r )
								upload_stack.append(True)

						else:
							upload_stack.append(True)

					if True not in upload_stack:
						success = True
						continue

					self.logger.info("[ %s/%s ] - Processing %s" % ( tries, self.max_retry, rname ))

					k = Key(to_buck)
					k.name = rname

					if cache_file:
						'''upload cached file'''
						k.set_metadata('Content-Type', content_type)
						k.set_contents_from_filename( cache_file )

					else:
						'''open remote URL and feed it to S3 directly'''
						http = urllib3.PoolManager(1)
						resp = http.request('GET', remote_url)

						if resp.status == 200:
							try:
								k.set_metadata('Content-Type', resp.headers['content-type'])
							except KeyError:
								pass

							k.set_contents_from_string( resp.data )
							resp.release_conn()
							del resp
							del http

						else:
							self.logger.warn("[ %s/%s ] - Failed fetch of %s (%s) %s" % ( tries, self.max_retry, rname, resp.status, resp.headers ))
							continue

					k.close()
					del k

					'''Check the work, since we streamed the bits'''
					upped_key = to_buck.get_key( rname )
					new_etag = upped_key.etag.replace('"','')
					upped_key.close()
					if new_etag != md5:
						self.logger.warn("[ %s/%s ] - MD5 mismatch for %s - %s %s" % ( tries, self.max_retry, rname, new_etag, md5 ))
						continue

					'''Any key with a "+", s3 treats like its a space, cheap hack'''
					if name.find("+") > 0:
						k1 = Key(to_buck)
						k1.key = rname

						'''fix for wget/curl type clients'''
						k1.copy( self.dest_bucket, rname.replace("+", " ") )
						self.logger.info("EXTRA copy as %s" % rname.replace("+"," "))

						'''fix for apt/iso-8859-1'''
						k1.copy( self.dest_bucket, rname.replace("+", "%2B") )
						self.logger.info("EXTRA copy as %s" % rname.replace("+","%2B"))

						k1.close()

					success = True
					del upped_key
					del new_etag

				except S3CreateError, e:
					self.logger.warn("Failed to create key %s" % name )
					self.logger.warn(e)

				except S3ResponseError, e:
					self.logger.warn("Got a bad response from S3 for  %s" % name )
					self.logger.warn(e)

				except BotoClientError, e:
					self.logger.warn("Encountered general boto client error %s" % e )

				except BotoServerError, e:
					self.logger.warn("Encountered general boto server error %s" % e )

				except StorageResponseError, e:
					self.logger.warn("Encountered general boto storage error %s" % e )

				except Exception, e:
					self.logger.warn("Failed upload for %s %s" % ( last_key, e ))

				finally:
					gc.collect()
					time.sleep(0.1)

			if item:
				self.queue.task_done()
				del item

			if not success:
				self.logger.warn("Failed to upload %s. Aborting upload to prevent inconsistent meta-data" % last_key )
				self.error.set()

		if self.error.is_set():
			self.logger.critical("Aborting file upload, error signal has been recieved")
