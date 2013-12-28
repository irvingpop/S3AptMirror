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

from aptmetaworker import MetaS3Worker
from http2s3worker import HTTP2S3Worker
from time import strftime
from datetime import date, timedelta
from s3uploadobj import S3UploadObject
from s3deleteworker import S3DeleteWorker

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

import boto.exception
import argparse
import gzip
import logging
import hashlib
import os
import Queue
import re
import signal
import shutil
import sqlite3
import sys
import tempfile
import time
import threading
import urllib3


class APTReleaseParser():

	def __init__(self, logger, tempdir=None):
		self.tempdir = tempdir

		# Hopefully we re-use the same logger here
		self.logger = logging.getLogger(logger)
		self.logger.info("apt parser thread created")


	def parse(self, url, url_base, key_name, key_base, queue):
		start_md5_re = re.compile('^MD5Sum:$')
		stop_md5_re = re.compile('^SHA1:$')
		match_re = re.compile('.*binary-amd64.*|.*binary-i386.*|.*source.*|.*sources.*|.*i18n.*')
		excluded_re = re.compile('.*debian-installer.*|.*(Sources|Packages)$')

		try:
			http = urllib3.PoolManager()
			success = False
			tries = 0
			max_try = 3
			meta = S3UploadObject()

			while not success and tries < max_try:
				tries += 1

				self.logger.info("Fetching %s" % url )
				resp = http.request('GET', url)

				if resp.status == 200:
					meta_fh, meta_cache = tempfile.mkstemp()
					meta.set_value('cache_file', meta_cache)
					meta.set_value('key_name', key_name)
					meta.set_value('download', False)
					meta.set_value('type', 'meta')

					try:
						meta.set_value("Content-Type", resp.headers['content-type'])
					except KeyError:
						self.logger.info("No content-type reported on %s" % key_name )

					f_local = os.fdopen( meta_fh, "w+b" )
					f_local.write( resp.data )
					f_local.close()

				if meta.same("size"):
					success = True

				else:
					self.logger.critical("Failed fetch on %s" % key_name)

				meta.md5_cache_file()
				queue.put(meta)

				started_md5 = False
				count = 0
				for line in resp.data.splitlines():

					if not started_md5:
						if start_md5_re.match( line ):
							started_md5 = True
							continue

					elif not stop_md5_re.match( line ):
						md5, size, kname = line.split()

						if match_re.match( kname ) and not excluded_re.match( kname ):
							anon = S3UploadObject()
							anon.set_value('remote_url', "%s/%s" % ( url_base, kname ))
							anon.set_value('key_name', "%s/%s" % ( key_base, kname ))
							anon.set_value('temp_name', key_name.replace('/','_') )
							anon.set_value('size', size)
							anon.set_value('remote_md5', md5)
							queue.put(anon)
							count += 1

					elif stop_md5_re.match(line):
						break

				self.logger.info("%s has MD5 of %s" % ( key_name, meta.get_value('md5')))
				self.logger.info("Found %s meta-items in %s" % ( count, key_name ))

		except Exception, e:
			print e

#queue = Queue.Queue()
#a = APTReleaseParser()
#host = 'archive.ubuntu.com'
#a.parse('http://archive.ubuntu.com/ubuntu/dists/lucid/Release', host, queue)

#print queue.qsize()
