import threading
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
import boto.exception
import logging
import Queue

class S3CopyWorker(threading.Thread):

	def __init__(self, queue, src_bucket, logger, creds, qtype="REGULAR", prestage=False, max_tries=3):
		self.queue = queue
		self.logger = logger
		self.src_bucket = src_bucket
		self.qtype = qtype
		self.prestage = False
		self.max_tries = max_tries
		threading.Thread.__init__(self)

		self.connection = S3Connection(creds[0], creds[1])

	def run(self):
		from_buck = self.connection.get_bucket(self.src_bucket)
		prestage = {}

		'''bucket pool'''
		bucket_pool = {}

		'''sync the files'''
		while not self.queue.empty():
			item = self.queue.get()
			name= str( item[0] )
			copy_name = name
			etag= str( item[1] )
			destination_buckets = item[2]
			success = False
			error_count = 0

			while not success and error_count < self.max_tries:

				for to_buck in destination_buckets:
					prestage[ to_buck ] = []

					try:
						if to_buck not in bucket_pool:
							bucket_pool[ to_buck ] = self.connection.get_bucket( to_buck )

					except boto.exception.AWSConnectionError, e:
						self.logger.debug("Unable to establish connection to S3\n%s" % e )
						thread.exit()

					except Exception,e :
						self.logger.critical("Error establishing connection to S3" % e )
						thread.interrupt_main()


					if self.prestage:
						copy_name = "%s-new" % name
						anon = name, copy_name
						prestage[ to_buck ].append( anon )

					try:
						to_key = bucket_pool[ to_buck ].get_key( name )
						if to_key is not None and to_key.exists:

							if to_key.etag == etag:
								self.queue.task_done()
								success = True
								continue
							else:
								self.logger.warn("%s for bucket %s has mismatched MD5" % ( name, to_buck ))

						from_key = Key( from_buck )
						from_key.key = name
						self.logger.debug("%s COPYING %s to %s" % (self.qtype, copy_name, to_buck ))
						from_key.copy(to_buck, copy_name)
						success = True
						self.queue.task_done()

					except boto.exception.S3CopyError, e:
						self.logger.debug("Error while copying %s to %s, will retry %s more times" % ( copy_name, to_buck, error_count ))
						error_count += 1

					except Exception, e:
						self.logger.debug("Unhandled exception\n%s" % e )
						thread.interrupt_main()

			if error_count == self.max_tries:
				self.logger.critical("Unable to synchronize queue. Aborting")
				thread.interrupt_main()

		'''flip prestaged files to the real name'''
		if self.prestage:
			self.logger.info("Flipping prestaged files to active name")

		for to_buck in prestage:
			self.logger.info("Flipping %s to active" % to_buck )
			for item in prestage[ to_buck ]:
				real_name = item[0]
				prestaged_name = item[1]

				prestaged_key = Key( to_buck )
				prestaged_key.key = prestaged_name
				prestage_key.copy( to_buckets, real_name )

			self.logger.info("Flipping done for %s" % to_buck )
