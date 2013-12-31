S3AptMirror
===========

Clone of Ben Howard's S3AptMirror on launchpad: https://code.launchpad.net/s3aptmirror

What it does
------------

The command apt2s3mirror will create Amazon S3 based APT repos by mirroring real APT repos.  It can perform a full or incremental synchronization.   This can be useful for rudimentary upstream package version management. 

This tool is very fast, able to move around all of Ubuntu's precise repos in less than 2 hours.  It has a configurable number of parallel workers (default 16), although you can use more if you have enough RAM.   In my experience, 16 workers is the maximum you want to use with a 4GB RAM machine.

NOTE:  apt2s3mirror will decide which files to mirror by reading the APT metadata files.  It won't copy every version of the package, just the ones referenced in the Packages files. 

Usage
-----

<pre class="console">
usage: apt2s3mirror [-h] [--server SERVER] [--distro DISTRO]
                    [--subrepos SUBREPOS] [--destination DESTINATION]
                    [--dir DIR] [--secret_key SECRET_KEY]
                    [--access_key ACCESS_KEY] [--meta_parse] [--meta_only]
                    [--no_meta] [--purge_old] [--delete_delay DELETE_DELAY]
                    [--log LOG] [--db_loc DB_LOC] [--silent] [--print_urls]
                    [--workers WORKERS]

optional arguments:
  -h, --help            show this help message and exit
  --server SERVER       Server URL to mirror from
  --distro DISTRO       Distributions to mirror(comma-separate list)
  --subrepos SUBREPOS   Sub-repos to mirror(comma-separate list)
  --destination DESTINATION
                        Destination bucket on S3
  --dir DIR             Bucket sub directory to store files
  --secret_key SECRET_KEY
                        AWS Secret Key
  --access_key ACCESS_KEY
                        AWS Access Key/ID
  --meta_parse          Only parse meta-data
  --meta_only           Process only meta data (should not be used)
  --no_meta             Upload new files, but skip meta data
  --purge_old           Purge files not referenced in current meta-data
  --delete_delay DELETE_DELAY
                        Number of days to wait before a file is eligible for
                        purging
  --log LOG             Name of the log file to log to
  --db_loc DB_LOC       Location to store the temp DB for processing meta-data
  --silent              Disable on-screen logging, useful for automated runs
  --print_urls          Print URLS to work on and exit
  --workers WORKERS     Number of workers to process meta-data

</pre>
