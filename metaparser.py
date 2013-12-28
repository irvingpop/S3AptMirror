#!/usr/bin/python
# vi: ts=4 noexpandtab

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 October 2011
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

## Mirrors apt repositories to Amazon S3
import sys

from boto.s3.key import Key
from time import strftime
from datetime import date, timedelta
from s3uploadobj import S3UploadObject
import argparse
import logging
import gzip
import hashlib
import os
import Queue
import re
import signal
import sys
import tempfile
import traceback
import time
import threading
import urllib3


class APTParserWorker(threading.Thread):
	"""
	Thread worker for parsing over a queue of URLs to get meta information for an APT repo
	"""

	def __init__(self, logger, work_queue, fetch_queue, meta_queue, tempdir, server, srcdir, dstdir, error):
		threading.Thread.__init__(self)
		self.server = server
		self.srcdir = srcdir
		self.dstdir = dstdir
		self.error = error
		self._stop = threading.Event()
		self.tempdir = tempdir  # where to cache the files
		self.work_queue = work_queue
		self.fetch_queue = fetch_queue	# return queue for regular debs
		self.meta_queue = meta_queue	# return queue for meta-data

        # Hopefully we re-use the same logger here
		self.logger = logging.getLogger(logger)
		self.logger.info("Worker thread created")

	def stop(self):
		self._stop.set()

	def stopped(self):
		return self._stop.isSet()

	def decoder(self, gz_data):
		"""
			Line iterator for a gzip object
		"""
		f = None

		try:
			f = gzip.open(gz_data, 'rb')
			for line in f.read().splitlines():
				yield line

		except Exception as e:
			self.logger.debug(traceback.format_exc(e))
			self.logger.debug("OH NOES!")

		finally:
			if f:
				f.close()


	def parse_src(self, gz_data, fetch_queue, fname):
		src_re = re.compile('.*Source.*gz$')
		src_pkg = re.compile('^Package:.*')				# New package header
		src_files = re.compile('^Files:.*')				# Section header for source files
		src_afiles = re.compile('.*dsc$|.*gz$')			# Actual files under section header for files
		src_bfiles = re.compile('^.*:.*$')				# Next section match
		src_dir = re.compile('^Directory:.*')			# Directory of the files

		count = 0
		currpkg = None
		currdir = None
		currfile = None
		reset = False

		try:

			for line in self.decoder(gz_data):
				if src_pkg.match( line ):
					currpkg = str( line.split(' ')[1] ).rstrip()
					currdir = None
					currfile = None

				elif currpkg and src_dir.match( line ):
					currdir = str( line.split(' ')[1] ).rstrip()

				elif currdir and src_files.match( line ):
					currfile = True

				elif currfile:
					if not src_bfiles.match( line ) and src_afiles.match( line ):
						md5, size, pname = line.split()
						s3obj  = S3UploadObject()
						s3obj.set_value("name", pname )
						s3obj.set_value("key_name", "%s/%s/%s" % ( self.dstdir, currdir, pname ))
						s3obj.set_value("remote_url", "http://%s/%s/%s/%s" % ( self.server, self.srcdir, currdir, pname ))
						s3obj.set_value("remote_md5", md5 )
						s3obj.set_value("remote_size", size )
						fetch_queue.put( s3obj )
						count += 1
					else:
						currpkg = None
						currdir = None
						currfile = None

			self.logger.info("Found %s items in %s" % (count, fname))

		except Exception, e:
			self.logger.info(traceback.format_exc(e))
			self.logger.info("Skipping file as a null file")
			return False


	def parse_pkg(self, gz_data, fetch_queue, fname):
		pkg_name = re.compile('^Package:.*')			# New package header
		pkg_file = re.compile('^Filename:.*')			# File name of the packages
		pkg_md5 = re.compile('^MD5sum:.*')				# MD5 of the package
		pkg_size = re.compile('^Size:.*')				# Package size

		reset = False
		currpkg = None
		currdir = None
		currfile = None
		count = 0

		try:

			for line in self.decoder(gz_data):

				if pkg_name.match( line ):
					currpkg = str( line.split(' ')[1] ).rstrip()

				elif pkg_file.match( line ) and currpkg is not None:
					currfile = str( line.split(' ')[1] ).rstrip()

				elif pkg_size.match( line ) and currfile is not None:
					currsize = str( line.split(' ')[1] ).rstrip()

				elif pkg_md5.match( line ) and currsize is not None:
					s3obj = S3UploadObject()
					s3obj.set_value("name", currfile)
					s3obj.set_value("key_name", "%s/%s" % ( self.dstdir, currfile ))
					s3obj.set_value("remote_url", "http://%s/%s/%s" % ( self.server, self.srcdir, currfile ))
					s3obj.set_value("remote_size", currsize )
					s3obj.set_value("remote_md5", str( line.split(' ')[1] ).rstrip() )
					fetch_queue.put( s3obj )
					count += 1

			self.logger.info("Found %s items in %s" % (count, fname))

		except Exception, e:
			self.logger.info(traceback.format_exc(e))
			self.logger.info("Skipping file as a null file")
			return False

	def parse_int_index(self, data, meta_queue, fname):
		skip = re.compile('^SHA1:$')
		key_base = fname.replace('/Index','')
		count = 0
		tran = None

		try:
			for line in data.splitlines():

				if skip.match( line ):
					continue

				if len(line.split()) != 3:
					continue

				sha1, size, lname = line.split()
				s3obj = S3UploadObject()
				s3obj.set_value("name", lname)
				s3obj.set_value("key_name", "%s/%s" % ( key_base, lname ))
				s3obj.set_value("remote_url", "http://%s/%s/%s" % ( self.server, key_base, lname ))
				s3obj.set_value("remote_size", size )
				s3obj.set_value("remote_sha1", sha1 )

				http = urllib3.PoolManager()
				tran = http.request('GET', s3obj.get_value('remote_url'), retries=4, preload_content=False)
				tran_fh, tran_cache = tempfile.mkstemp( prefix=s3obj.get_value('key_name').replace("/","_"), dir=self.tempdir)

				f_local = os.fdopen( tran_fh, "w+b" )
				f_local.write( tran.data )
				f_local.close()

				try:
					s3obj.set_value('Content-Type', tran.headers['content-type'])
				except KeyError:
					pass

				s3obj.set_value("cache_file", tran_cache)
				s3obj.md5_cache_file()

				count += 1
				meta_queue.put( s3obj )

			self.logger.info("Found %s items in %s" % ( count, fname ))

		except Exception, e:
			self.logger.critical(traceback.format_exc(e))
			self.logger.critical("Error while parsing international index %s" % fname)
			self.error.set()
			raise

		finally:
			if tran:
				tran.release_conn()
				tran = None

	def run(self):
		key = Key()		# Use Boto for getting MD5 checksums
		http = urllib3.PoolManager()
		source_re = re.compile('.*(Sources,source).gz$')
		pkg_re = re.compile('.*Packages.gz$')
		src_re = re.compile('.*Source.*gz$')
		index_re = re.compile('.*/i18n/Index$')

		while not self.work_queue.empty() and not self.stopped() and not self.error.is_set():
			meta = None

			try:
				meta = self.work_queue.get()

			except Queue.Empty, e:
				breaK

			'''fetch the remote meta-data'''
			success = False
			tries = 0
			max_try = 3
			content = None

			if not meta.get_value('download') and meta.get_value('type') == 'meta':
				'''i.e. don't re-process the release file, because it won't work'''
				self.meta_queue.put( meta )
				success = True
				self.work_queue.task_done()
				continue

			while not success and tries < max_try:

				tries += 1
				self.logger.info("Attempt [ %s/%s ]: Fetching %s" % ( tries, max_try, meta.get_value('remote_url') ))
				resp = None

				try:

					resp = http.request('GET', meta.get_value('remote_url'), preload_content=False)

					if resp.status == 200:
						'''only write the file if we get a good status code'''
						meta_fh, meta_cache = tempfile.mkstemp( prefix=meta.get_value('temp_name'), dir=self.tempdir )
						meta.set_value("cache_file", meta_cache)

						try:
							meta.set_value("Content-Type", resp.headers['content-type'])
						except KeyError:
							pass

						f_local = os.fdopen( meta_fh, "w+b" )
						f_local.write( resp.data )
						f_local.close()

						'''check the file'''
						meta.set_value('size', str( os.stat(meta_cache)[6] ))
						meta.set_value('remote_size', str( resp.headers['content-length'] ))
						meta.md5_cache_file()

						if meta.get_value('remote_md5') and meta.get_value('md5'):
							if not meta.same("md5"):
								self.error.set()
								raise Exception("Corrupt MD5 for %s (R) %s (L) %s" % ( meta.get_value('key_name'), meta.get_value('remote_md5'), meta.get_value('md5')))

						if meta.get_value('size'):
							if meta.same("size"):
								success = True
								self.logger.info("Attempt [ %s/%s ]: %s download is good"  % ( tries, max_try, meta.get_value('remote_url')  ))
							else:
								self.error.set()
								raise Exception("Bad Size detected for %s" % meta.get_value('key_name'))
						else:
							sucess = True

					elif resp.status == 400 or resp.status == 404:
						self.logger.warn("Attempt [ %s/%s ]: %s does not exist, excluding from set"  % ( tries, max_try, meta.get_value('remote_url') ))
						remote_url_processing_done = True
						success = True
						continue

					else:
						self.logger.warn("Attempt [ %s/%s ]: %s received status %s"  % ( tries, max_try, meta.get_value('remote_url'), resp.status ))


					'''md5 the file'''
					self.logger.info("MD5 of %s computed for %s" % ( meta.get_value('md5'), meta.get_value('key_name') ))

					if resp.data:
						if src_re.match(meta.get_value('key_name') ):
							self.logger.info("Parsing %s as source meta-data" % meta.get_value('key_name'))
							self.parse_src(meta_cache, self.fetch_queue, meta.get_value('key_name'))

						elif pkg_re.match(meta.get_value('key_name') ):
							self.logger.info("Parsing %s as package meta-data" % meta.get_value('key_name'))
							self.parse_pkg(meta_cache, self.fetch_queue, meta.get_value('key_name'))

						elif index_re.match(meta.get_value('key_name') ):
							self.logger.info("Parsing %s as internationalization index file" % meta.get_value('key_name'))
							self.parse_int_index(meta_cache, self.meta_queue, meta.get_value('key_name'))

					self.meta_queue.put( meta )

				except Exception, e:
					self.logger.debug("WTF? %s" % e)
					self.error.set()
					self.logger.critical( meta.dump() )
					raise

				finally:
					if resp:
						resp.release_conn()
						resp = None
					self.work_queue.task_done()
