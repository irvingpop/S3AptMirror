#!/usr/bin/python
# vi: ts=4 noexpandtab

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 October 2011
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

from boto.s3.key import Key

"""
S3UploadObject is a class that provides a way to describe an object for upload to S3
"""

class S3UploadObject():

	def __init__(self, name=None, cache_file=None, remote_url=None, md5=None, md5_encoded=None,
			remote_md5=None, remote_md5_encoded=None, size=None, temp_name=None, key_name=None, content_type=None):

		"""
		Standard attributes of a file destined for S3 upload
		"""
		self.obj = {}
		self.obj['name'] = name
		self.obj['key_name'] = key_name
		self.obj['temp_name'] = temp_name
		self.obj['cache_file'] = cache_file
		self.obj['remote_url'] = remote_url
		self.obj['md5'] = md5
		self.obj['md5_encoded'] = md5_encoded
		self.obj['remote_md5'] = remote_md5
		self.obj['remote_md5_encoded'] = remote_md5_encoded
		self.obj['size'] = size
		self.obj['remote_size'] = size
		self.obj['Content-Type'] = content_type

	def get_value(self, name):
		"""
		return values
		"""

		try:
			return self.obj[name]

		except KeyError, e:
			return None

	def set_value(self, name, value):
		self.obj[name] = value

	def same(self, name):
		"""
		Compares local to remote values
		"""

		try:
			if self.obj[name] == self.obj[ "remote_%s" % name ]:
				return True
			else:
				return False

		except KeyError, e:
			return False

	def get_value_pairs(self, name):
		"""
		Returns local and remote values if set
		"""

		try:
			return self.obj[name], self.obj["remote_%s" % name]

		except KeyError, e:
			return False

	def set_md5_pair(self,md5, encoded):
		"""
		Set MD5 pair of hex and base64 encoded MD5 from a tuple)
		"""
		self.obj['md5'] = md5
		self.obj['md5_encoded'] = encoded

	def get_md5(self):
		return self.obj['md5'], self.obj['md5_encoded']

	def md5_cache_file(self):
		"""
		Compute the MD5 using BOTO
		"""

		key = Key()

		if self.obj['cache_file']:

			fname = self.obj['cache_file']
			f_open = open( fname )
			self.obj['md5'], self.obj['md5_encoded'] = key.compute_md5( f_open )
			f_open.close()

		else:
			raise Exception("VALUE_NOT_SET", "cache_file has not been set")

		key.close()
		del key

	def dump(self):
		return self.obj
