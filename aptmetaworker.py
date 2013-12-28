#!/usr/bin/python
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
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
from datetime import date, timedelta
from s3uploadobj import S3UploadObject
import urllib3
import boto.exception
import logging
import Queue
import time
import re
import os

class MetaS3Worker(threading.Thread):

	def __init__(self, queue, dest_bucket, logger, creds, error, max_retry=3):
		threading.Thread.__init__(self)
		self.setDaemon(True)
		self.queue = queue
		self.dest_bucket = dest_bucket
		self.creds = creds
		self.error = error
		self.bucket = None
		self.max_retry = max_retry		# How many times to try and fetch the file
		self.rollback_md5 = {} 		# Used to store keys/md5 for rollback

        # Hopefully we re-use the same logger here
		self.logger = logging.getLogger(logger)
		self.logger.info("Worker thread created")

		try:
			self.conn = S3Connection(creds[0], creds[1])

			if self.conn is None:
				raise Exception("S3_NO_CONN", "S3 connection failed")

			else:
				self.logger.debug("Connection to S3 established")

			self.bucket = self.conn.get_bucket( self.dest_bucket)

			if self.bucket is None:
				raise Exception("NO_BUCKET", "Unable to get bucket %s" % self.dest_bucket )

			else:
				self.logger.debug("Fetched bucket %s" % self.dest_bucket )

		except Exception, e:
			self.logger.error("Exception during establishment of S3 Connection")
			self.logger.error(e)
			self.error.set()

	def backup_meta(self, batch):
		"""
		Backup the existing meta-data
		"""
		tagged_date = date.today()
		purge_date = tagged_date + timedelta(days=3)
		tries = 0
		backup_ext = date.today()


		self.logger.info("Starting backup process")
		'''create a backup of the existing meta-data'''
		for k in batch:

			if self.error.is_set():
				return False

			self.logger.debug("backing up %s" % ( k.get_value('key_name') ) )
			file_success = False
			file_try = 0
			bucket = self.conn.get_bucket( self.dest_bucket )
			key = bucket.get_key( k.get_value('key_name') )

			'''Only backup existing keys'''
			try:

				if key is not None and key.exists():
					k.set_value("remote_md5", key.etag.replace('"',''))
					self.logger.debug("META INFO %s %s %s" % ( k.get_value("md5"), k.get_value("remote_md5"), k.get_value('key_name')))

				else:
					self.logger.debug("%s is a new meta-data file, no backup needed" % k.get_value('key_name'))
					continue

			except Exception, e:
				self.logger.debug("Unable to allocate boto key for %s, aborting\n%s" % ( k.get_value('key_name'), e ) )
				self.error.set()
				raise

			while not file_success and file_try <= self.max_retry:
				file_try += 1

				if k.get_value("md5") != k.get_value("remote_md5"):
					self.logger.debug("Attempt [ %s/%s ] Backing up %s to %s-[%s,rollback]" % ( file_try, self.max_retry,
						k.get_value('key_name'), k.get_value('key_name'), backup_ext ))

					try:
						meta = {}
						meta["delete_ordinal"] = str( purge_date.toordinal() )
						meta["delete_on"] = str( purge_date )
						meta["delete_tagged_on"] = str( tagged_date )

						key.copy( self.dest_bucket, "%s-%s" % ( k.get_value('key_name'), backup_ext ), metadata=meta)
						key.copy( self.dest_bucket, "%s-latest" % k.get_value('key_name'), metadata=meta )
						self.logger.debug("Attempt [ %s/%s ] Backed-up %s as %s-[%s,rollback]" % ( file_try,
							self.max_retry, k.get_value('key_name'), k.get_value('key_name'), backup_ext ))


					except Exception, e:
						self.logger.debug("Attempt [ %s/%s ] Failed to backup key %s\n%s" % ( file_try, self.max_retry, k.get_value('key_name'), e ))
						self.logger.critical(e)
						continue

					self.rollback_md5[ "%s-latest" % k.get_value('key_name') ] = key.name, key.etag.replace('"','')
					self.logger.debug("Attempt [ %s/%s ] Preserved %s in rollback-queue" % ( file_try, self.max_retry, k.get_value('key_name') ))

				else:
					self.logger.debug("Attempt [ %s/%s ] Update for %s is not needed" % ( file_try, self.max_retry, k.get_value('key_name') ))
					k.set_value('skip', 'yes')
					batch.remove(k)

				file_success = True

			if not file_success:
				self.error.set()
				return False

		return True

	def upload_new_meta(self, batch):
		"""
		Upload new meta-data with '-new' suffix
		"""

		'''upload the meta-data'''
		tries = 0
		self.logger.debug("%s Beginning upload of meta-data" % self.name )

		'''upload a copy for staging'''
		for k in batch:

			if self.error.is_set():
				return False

			if k.get_value('skip') == 'yes':
				continue


			file_success = False
			file_try = 0

			self.logger.info("Uploading %s-new" % ( k.get_value('key_name') ))

			while not file_success and file_try != self.max_retry and not self.error.is_set():
				file_try += 1
				self.logger.info("Attempt [ %s/%s ] for %s-new" % ( file_try, self.max_retry, k.get_value('key_name') ))
				key = None

				try:

					key = Key( self.bucket )
					key.name = "%s-new" % k.get_value('key_name')

					try:
						if k.get_value('Content-Type'):
							key.set_metadata('Content-Type', k.get_value('Content-Type'))
						else:
							self.logger.warn("No Content-Type meta-data available for %s" % k.get_value('key_name'))
					except KeyError:
						self.logger.warn("No Content-Type meta-data available for %s" % k.get_value('key_name'))

					key.set_contents_from_filename(k.get_value('cache_file'), md5=k.get_md5())
					file_success = True

				except Exception, e:
					self.logger.info("Attempt [ %s/%s ] Failed upload of %s\n%s\n%s" % ( file_try, self.max_retry, k.get_value('key_name'), e, k.dump() ))

			if file_try == self.max_retry and not file_success:
				self.logger.warn("Attempt [ %s/%s ] Max retries on %s exceeded. Aborting." % ( file_try, self.max_retry, k.get_value('key_name') ))
				self.error.set()

			try:
				os.unlink(k.get_value('cache_file'))
			except IOError:
				self.logger.warn("Failed to remove file %s" % k.get_value('cache_file'))

		return True

	def flip_meta(self, batch):
		"""
		Renames meta-data files from "%s-new" to "%s"
		"""
		first_flip = []
		second_flip = []
		flips = first_flip, second_flip

		first_re = re.compile('.*/Release$|.*/Release\.gpg$|.*/Content-(i386|amd64)\.gz$|.*Packages.bz2')

		for k in batch:

			if k.get_value('skip') != 'yes':

				if first_re.match( k.get_value('key_name') ):
					first_flip.append( k )
				else:
					second_flip.append( k )

		self.logger.info("Flipping meta-data active...silently flipping for speed")

		for flip in flips:
			flipped = []
			for k in flip:

				if self.error.is_set():
				 	return False

				tries = 0
				max_tries = 3
				success = False
				last_key = None
				wait_time = 1
				key_name = k.get_value('key_name')

				try:
					while not success and tries < max_tries:
						"""
						this process needs to be as _fast_ as possible to prevent meta-data mismatches
						"""

						tries += 1
						key = self.bucket.copy_key(key_name, self.dest_bucket, "%s-new" % key_name)
						flipped.append("%s" % key_name )
						success = True

				except Exception, e:

					if tries >= max_tries:
						self.critical("Max retries for flipping %s exceeded.\n%s" % (key_name, e))
						return False

					else:
						self.logger.warn("Error encountered while flipping %s, sleeping for %s seconds and continuing\n%s" % ( key_name, wait_time, e ))
						time.sleep(wait_time)
						wait_time += 1			## AWS wants exponential back-off, but we can't be that patient
						self.logger.info("Continuing attempt to flip starting with %s" % key_name )

					pass

				if not success:
					self.logger.critical("Failed to flip-meta data. This is bad.")
					return False


			for key_name in flipped:
				self.logger.debug("Flipped %s: %s" % ( self.dest_bucket, key_name ))

		self.logger.info("Finished flipping meta-data active")
		self.logger.info("Removing staging meta-data files")

		for k in batch:

			try:
				key = self.bucket.get_key( "%s-new" % k.get_value('key_name') )

				if key.exists():
					self.logger.debug("Removed %s" % key.name )
					key.delete()

			except Exception, e:
				self.logger.warn("Non-critical error: unable to remove staging key %s\n%s" % ( key.name, e ) )
				pass

		self.logger.info("Finished removing staging meta-data files")

		return True


	def rollback(self):
		"""
		Rollback functionality. If this is invoked, then it is probably broken
		"""
		for k in self.rollback_md5:

			try:
				key_backed_up =  self.bucket.get_key( k )
				key_restore_to = self.bucket.get_key( self.rollback_md5[k][0] )

				if not key_restore_to:
					continue

				key_restore_to_etag = key_restore_to.etag.replace('"', '')

				if not key_backed_up.exists():
					self.logger.critical("ROLLBACK KEY of %s is MISSING. MIRROR IS NOW CORRUPT" % k )
					return False

				elif key_restore_to_etag != self.rollback_md5[k][1]:
					meta = []
					key.backed_up.copy( k.get_value('key_name'), metadata = meta )
					self.logger.critical("RESTORED KEY %s" % self.rollback_md5[k][0] )

					key.retore_to = self.bucket.get_key( k )
					key_restore_to_etag = key_restore_to.etag.replace('"', '')

				if key_restore_to_etag != k.get_value('key_name'):
					self.logger.critical("ROLLBACK OF %s HAS FAILED! MIRROR IS NOW CORRUPT" % k.get_value('cache_file') )
					return False
				else:
					self.logger.info("ROLLBACK OF %s SUCCEEDED" % k.get_value('cache_file') )

			except Exception, e:
				self.logger.critical("EXCEPTION ENCOUNTERED DURING ROLLBACK. THIS MIRROR IS CORRUPT!\n%s" % e )
				return False

		return True


	def run(self):
		"""
		This streams items over HTTP directly to S3 from a Queue comprising tuples of path and MD5 sum

		Since this is updating live meta-data, this is intentionally atomic (as atomic as S3 can be) and
		inefficient on uploads.

		This reads a Queue of tuples. Each tuple should have four or five elements. The structure should be:
			1. The local path of the meta-data file
			2. The remote name for the meta-data file
			3. The original file location of the meta-data
			4. The MD5 hex checksum of the meta-data file
			5. (OPTIONAL) The MD5 encoded checksum of the meta-data file
		"""

		self.logger.debug("Found %s APT META Data batches for processing %s" % ( self.queue.qsize(), self.queue.empty() ))

		while not self.queue.empty():

			try:
				batch = self.queue.get()
				self.logger.info("Starting backup of Meta-data")

				if self.backup_meta( batch ):
					self.logger.info("Finished backup of Meta-data")
					self.logger.info("Uploading new meta-data")

					if self.upload_new_meta(batch):
						self.logger.info("Finished Upload of new Meta-data")
						self.logger.info("Activating new meta-data")

						if self.flip_meta( batch ):
							self.logger.info("Activated new meta-data")

						else:
							self.logger.critical("FAILED to flip new meta-data")
							self.rollback()
							self.error.set()

					else:
						self.logger.critical("FAILED new-meta upload")
						self.error.set()
				else:
					self.logger.critical("FAILED meta-data backup")
					self.error.set()

			except Exception, e:
				self.logger.warn(e)
				self.error.set()
				raise

			finally:
				self.logger.info("Marking queue task done")
				self.queue.task_done()


		self.logger.info("Work has finished for this thread")
